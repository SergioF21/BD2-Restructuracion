#!/usr/bin/env python3
"""
Parser SQL que devuelve AST/ExecutionPlan sin side-effects.
"""

import sys
import os
from typing import Dict, List, Any, Optional, Union
from lark import Lark, Transformer, Token, Tree
from lark.exceptions import LarkError

# Importar la gramática
from grammar import GRAMMAR

class ExecutionPlan:
    """Representa un plan de ejecución para una consulta SQL."""
    
    def __init__(self, operation: str, **kwargs):
        self.operation = operation  # 'CREATE_TABLE', 'SELECT', 'INSERT', 'DELETE', 'UPDATE'
        self.data = kwargs
    
    def __repr__(self):
        return f"ExecutionPlan({self.operation}, {self.data})"


class SQLTransformer(Transformer):
    """
    Transformer robusto: usa *items para evitar errores de aridad,
    normaliza tokens a tipos nativos y construye ExecutionPlan consistentes.
    """

    # --- Helpers para convertir tokens ---
    def _to_str(self, v):
        if isinstance(v, Token):
            return str(v)
        return v

    def _to_number(self, v):
        if isinstance(v, Token):
            s = str(v)
            return float(s) if '.' in s else int(s)
        return v

    def statement_list(self, items):
        """Debug para ver todo lo que se está parseando."""
        print(f"DEBUG statement_list ALL ITEMS: {items}")
        return {"type": "statement_list", "statements": items}

    def _unwrap(self, item):
        # Si Lark nos pasa Tree o Token anidado, sacamos el valor contenido cuando sea simple.
        if isinstance(item, Tree):
            # si es un Tree con un único child token -> devolver child valor
            if len(item.children) == 1 and isinstance(item.children[0], Token):
                tok = item.children[0]
                if tok.type in ("INT", "SIGNED_NUMBER"):
                    return self._to_number(tok)
                return str(tok)
            # si Tree representa lista (p.ej. field_definitions) devolver children ya transformados
            return [self._unwrap(c) for c in item.children]
        if isinstance(item, Token):
            if item.type in ("INT",):
                return int(item)
            if item.type in ("SIGNED_NUMBER",):
                return self._to_number(item)
            if item.type in ("ESCAPED_STRING",):
                s = str(item)
                return s[1:-1].encode('utf-8').decode('unicode_escape')
            return str(item)
        return item

    def _unwrap_token(self, v):
        if isinstance(v, Token):
            if v.type in ("INT",):
                return int(str(v))
            if v.type in ("SIGNED_NUMBER",):
                s = str(v); return float(s) if '.' in s else int(s)
            if v.type == "ESCAPED_STRING":
                s = str(v); return s[1:-1].encode('utf-8').decode('unicode_escape')
            return str(v)
        if isinstance(v, Tree):
            # si Tree tiene un solo child token, desempaquetar
            if len(v.children) == 1 and isinstance(v.children[0], Token):
                return self._unwrap_token(v.children[0])
            # si Tree es lista, procesar children
            return [self._unwrap_token(c) for c in v.children]
        return v

    def value_list(self, items):
        """Value list corregido para aplanar listas."""
        values = []
        for item in items:
            unwrapped = self._unwrap_tree_token(item)
            if isinstance(unwrapped, list):
                values.extend(unwrapped)
            else:
                values.append(unwrapped)
        
        print(f"DEBUG value_list final: {values}")
        return values

    def index_type(self, items):
        """Procesa tipo de índice."""
        if items:
            return self._unwrap_tree_token(items[0])
        return None

    def key_field(self, items):
        """Procesa campo clave."""
        if items:
            return self._unwrap_tree_token(items[0])
        return None

    # --- Start / statement list ---
    def start(self, items):
        if len(items) == 1:
            return items[0]
        return {"type": "statement_list", "statements": items}

    def statement_list(self, items):
        return {"type": "statement_list", "statements": items}

    # --- CREATE TABLE from schema ---
    def create_table_schema(self, *items):
        # tolerant: buscar CNAME (table name) y field_definitions (lista)
        table_name = None
        fields = None
        for it in items:
            if isinstance(it, str):
                if table_name is None:
                    table_name = it
            elif isinstance(it, list):
                fields = it
            elif isinstance(it, dict) and 'name' in it:
                # single field
                fields = [it] if fields is None else (fields + [it])
        return ExecutionPlan("CREATE_TABLE", table_name=table_name, fields=fields, source=None)

    def create_table_statement(self, items):
        # items may contain table name (Token or str) and a list/tree of field definitions
        table_name = None
        fields = []
        from lark import Token, Tree
        for it in items:
            if isinstance(it, Token) and it.type == "CNAME" and table_name is None:
                table_name = str(it)
            elif isinstance(it, str) and table_name is None:
                table_name = it
            elif isinstance(it, list):
                # ya deberían ser dicts de field_definition
                for f in it:
                    fields.append(f)
            elif isinstance(it, Tree) and it.data == "field_definitions":
                # desempaca children
                for child in it.children:
                    # si child es Tree o Token, intenta convertir
                    fields.append(self._unwrap_tree_token(child))
            elif isinstance(it, dict) and 'name' in it:
                fields.append(it)
        return ExecutionPlan('CREATE_TABLE', table_name=table_name, fields=fields or None, source=None)


    def create_table_from_file(self, items):
        """CREATE TABLE FROM FILE corregido."""
        table_name = None
        file_path = None
        index_type = None
        key_field = None
        
        print(f"DEBUG create_table_from_file items: {items}")
        
        # Procesar todos los items
        for item in items:
            unwrapped = self._unwrap_tree_token(item)
            print(f"DEBUG unwrapped: {unwrapped} (type: {type(unwrapped)})")
            
            if isinstance(unwrapped, str):
                if table_name is None:
                    table_name = unwrapped
                elif file_path is None and ('.csv' in unwrapped or unwrapped.endswith('.csv')):
                    file_path = unwrapped
                elif index_type is None and unwrapped.upper() in ['BTREE', 'EXTENDIBLEHASH', 'ISAM', 'SEQ', 'RTREE']:
                    index_type = unwrapped.upper()
                elif key_field is None and unwrapped not in [table_name, file_path, index_type]:
                    key_field = unwrapped
        
        # Si todavía no tenemos key_field, buscar en lists anidadas
        if key_field is None:
            for item in items:
                unwrapped = self._unwrap_tree_token(item)
                if isinstance(unwrapped, list):
                    for subitem in unwrapped:
                        if isinstance(subitem, str) and subitem not in [table_name, file_path, index_type]:
                            key_field = subitem
                            break
        
        print(f"DEBUG final: table_name={table_name}, file_path={file_path}, index_type={index_type}, key_field={key_field}")
        
        return ExecutionPlan('CREATE_TABLE', 
                        table_name=table_name, 
                        fields=None, 
                        source=file_path, 
                        index_type=index_type, 
                        key_field=key_field)
        
    # --- field definition and list ---
    def field_definition(self, items):
        """Field definition mejorado para manejar Trees vacíos."""
        print(f"DEBUG field_definition items: {items}")
        
        if len(items) < 2:
            return None
        
        name = self._unwrap_tree_token(items[0])
        dtype_info = items[1]
        
        dtype = None
        size = 0
        
        # DEBUG profundo del dtype_info
        print(f"DEBUG dtype_info: {dtype_info} (type: {type(dtype_info)})")
        if hasattr(dtype_info, 'data'):
            print(f"DEBUG dtype_info.data: {dtype_info.data}")
            print(f"DEBUG dtype_info.children: {dtype_info.children}")
        
        # Estrategia 1: Si es Tree de data_type vacío, buscar en el contexto
        if dtype_info is None or (hasattr(dtype_info, 'data') and dtype_info.data == 'data_type' and not dtype_info.children):
            # Buscar en items siguientes si hay algún tipo
            for i in range(2, len(items)):
                potential = self._unwrap_tree_token(items[i])
                if isinstance(potential, str) and potential.upper() in ['INT', 'VARCHAR', 'FLOAT', 'DATE', 'ARRAY']:
                    dtype = potential
                    break
        
        # Estrategia 2: Procesar normal
        else:
            dtype_result = self._unwrap_tree_token(dtype_info)
            print(f"DEBUG dtype_result: {dtype_result} (type: {type(dtype_result)})")
            
            if isinstance(dtype_result, tuple):
                dtype, size = dtype_result
            elif isinstance(dtype_result, str):
                dtype = dtype_result
        
        # Estrategia 3: Si todavía no tenemos dtype, usar lógica de backup
        if dtype is None:
            # Para el campo 'id', es probable que sea INT
            if name.lower() == 'id':
                dtype = 'INT'
            # Para campos con 'nombre', 'descripcion', etc, podría ser VARCHAR
            elif any(x in name.lower() for x in ['nombre', 'name', 'desc', 'description']):
                dtype = 'VARCHAR'
                size = 50  # tamaño por defecto
        
        # Procesar opciones de índice
        index_type = None
        if len(items) > 2:
            for i in range(2, len(items)):
                item = items[i]
                unwrapped = self._unwrap_tree_token(item)
                if isinstance(unwrapped, str) and unwrapped.upper() in ['SEQ', 'BTREE', 'EXTENDIBLEHASH', 'ISAM', 'RTREE']:
                    index_type = unwrapped.upper()
                    break
        
        result = {
            "name": name,
            "type": dtype,
            "size": size,
            "index": index_type
        }
        print(f"DEBUG field_definition final result: {result}")
        return result

    
    def comparison_operator(self, items):
        """Procesa operadores de comparación."""
        print(f"DEBUG comparison_operator items: {items}")
        
        if items:
            operator = self._unwrap_tree_token(items[0])
            print(f"DEBUG comparison_operator result: {operator}")
            return operator
        
        # Si está vacío, podría ser que el operador viene de otra manera
        return "="  # Default

    def between_condition(self, items):
        """Procesa condiciones BETWEEN."""
        print(f"DEBUG between_condition items: {items}")
        
        if len(items) >= 4:
            field = self._unwrap_tree_token(items[0])
            start = self._unwrap_tree_token(items[2])  # Saltar BETWEEN
            end = self._unwrap_tree_token(items[4])    # Saltar AND
            
            return {
                "type": "between",
                "field": field,
                "start": start,
                "end": end
            }
        return None

    def spatial_condition(self, items):
        """Procesa condiciones espaciales IN (point, radius)."""
        print(f"DEBUG spatial_condition items: {items}")
        
        if len(items) >= 4:
            field = self._unwrap_tree_token(items[0])
            point = self._unwrap_tree_token(items[3])  # Después de IN (
            radius = self._unwrap_tree_token(items[5]) # Después de la coma
            
            return {
                "type": "spatial",
                "field": field,
                "point": point,
                "radius": radius
            }
        return None

    def EQUALS(self, token):
        """Procesa operador =."""
        return "="

    def NOTEQUALS(self, token):
        """Procesa operador !=."""
        return "!="

    def LESSTHAN(self, token):
        """Procesa operador <."""
        return "<"

    def GREATERTHAN(self, token):
        """Procesa operador >."""
        return ">"

    def LESSEQUAL(self, token):
        """Procesa operador <=."""
        return "<="

    def GREATEREQUAL(self, token):
        """Procesa operador >=."""
        return ">="

    

    def field_definitions(self, *items):
        # items are field_definition dicts
        fields = []
        for it in items:
            if isinstance(it, dict) and 'name' in it:
                fields.append(it)
            elif isinstance(it, list):
                for sub in it:
                    if isinstance(sub, dict) and 'name' in sub:
                        fields.append(sub)
        return fields

    # index options
    def index_options(self, *items):
        # return last item as index type (string)
        if not items:
            return None
        last = items[-1]
        if isinstance(last, Token):
            return str(last).upper()
        return str(last).upper()

    # --- SELECT ---
    def select_all(self, *items):
        return ["*"]

    def select_list(self, *items):
        return [self._unwrap(i) for i in items]

    def select_statement(self, items):
        sel = None
        table = None
        where = None
        from lark import Token, Tree
        for it in items:
            # select_list puede venir como list o Tree('select_all')
            if isinstance(it, list):
                sel = [self._unwrap_tree_token(x) for x in it]
            elif isinstance(it, Tree) and it.data == 'select_all':
                sel = ['*']
            elif isinstance(it, Token) and it.type == 'CNAME':
                table = str(it)
            elif isinstance(it, str) and table is None:
                table = it
            elif isinstance(it, dict) and it.get('type') in ('cmp','and','or','between'):
                where = it
        return ExecutionPlan('SELECT', table_name=table, select_list=sel or ['*'], where_clause=where)



    # comparisons / conditions
    def comparison(self, items):
        """Procesa comparaciones corregido."""
        print(f"DEBUG comparison items: {items}")
        
        if len(items) >= 3:
            field = self._unwrap_tree_token(items[0])
            operator_tree = items[1]
            value = self._unwrap_tree_token(items[2])
            
            # Procesar operador específicamente
            operator = "="  # default
            if hasattr(operator_tree, 'data') and operator_tree.data == 'comparison_operator':
                if operator_tree.children:
                    operator = self._unwrap_tree_token(operator_tree.children[0])
                else:
                    # Si el Tree está vacío, asumir "="
                    operator = "="
            else:
                operator = self._unwrap_tree_token(operator_tree)
            
            result = {
                "type": "comparison", 
                "field": field,
                "operator": operator,
                "value": value
            }
            print(f"DEBUG comparison result: {result}")
            return result
        
        return None

    def condition(self, items):
        """Procesa condiciones (puede ser simple o compuesta)."""
        print(f"DEBUG condition items: {items}")
        
        if len(items) == 1:
            # Condición simple
            return self._unwrap_tree_token(items[0])
        elif len(items) >= 3:
            # Condición compuesta (AND/OR)
            left = self._unwrap_tree_token(items[0])
            operator = self._unwrap_tree_token(items[1]).lower()
            right = self._unwrap_tree_token(items[2])
            
            return {
                "type": operator,
                "left": left,
                "right": right
            }
        
        return None

    def between(self, *items):
        # field, a, AND, b
        field = self._unwrap(items[0])
        a = self._unwrap(items[1])
        b = self._unwrap(items[-1])
        return {"type":"between", "field": field, "start": a, "end": b}

    # --- INSERT ---
    def insert_statement(self, items):
        table = None
        values = []
        from lark import Token, Tree
        for it in items:
            if isinstance(it, Token) and it.type == "CNAME" and table is None:
                table = str(it)
            elif isinstance(it, str) and table is None:
                table = it
            elif isinstance(it, list):
                # lista de valores (Tree nodes)
                values = [self._unwrap_tree_token(v) for v in it]
            elif isinstance(it, Tree) and it.data == 'value_list':
                values = [self._unwrap_tree_token(c) for c in it.children]
        return ExecutionPlan('INSERT', table_name=table, values=values or [])



    # --- UPDATE ---
    def assignment(self, *items):
        # field = value
        if len(items) >= 2:
            field = self._unwrap(items[0])
            val = self._unwrap(items[-1])
            return (field, val)
        return None

    def assignment_list(self, *items):
        return [i for i in items if i is not None]

    def update_statement(self, *items):
        table = None
        assigns = None
        where = None
        for it in items:
            if isinstance(it, str) and table is None:
                table = it
            if isinstance(it, list):
                # assignments
                assigns = [a for a in it if a is not None]
            if isinstance(it, dict) and it.get('type') in ('cmp','and','or','between'):
                where = it
        return ExecutionPlan("UPDATE", table_name=table, assignments=assigns or [], where_clause=where)

    # --- DELETE ---
    def delete_statement(self, items):
        """DELETE corregido - asigna where_clause correctamente."""
        print(f"DEBUG delete_statement items: {items}")
        
        table = None
        where = None
        
        for it in items:
            unwrapped = self._unwrap_tree_token(it)
            print(f"DEBUG delete item: {it} -> {unwrapped}")
            
            if isinstance(unwrapped, str) and table is None:
                table = unwrapped
            elif isinstance(unwrapped, dict) and unwrapped.get('type') == 'comparison':
                where = unwrapped
                print(f"DEBUG found where clause: {where}")
        
        print(f"DEBUG delete final: table={table}, where={where}")
        
        # Asegurarse de devolver el where_clause
        return ExecutionPlan('DELETE', table_name=table, where_clause=where)

    def where_clause(self, items):
        """Procesa WHERE clause."""
        print(f"DEBUG where_clause input: {items}")
        
        if items:
            condition = self._unwrap_tree_token(items[0])
            print(f"DEBUG where_clause result: {condition}")
            return condition
        return None

    # --- point / radius / values helpers ---
    def point(self, *items):
        nums = []
        for it in items:
            if isinstance(it, Token):
                nums.append(self._to_number(it))
            elif isinstance(it, (int, float)):
                nums.append(float(it))
        if len(nums) >= 2:
            return (nums[0], nums[1])
        return None

    def radius(self, val):
        return self._to_number(val)

    def string_literal(self, s):
        if isinstance(s, Token):
            sval = str(s)
            return sval[1:-1].encode('utf-8').decode('unicode_escape')
        return s

    def SIGNED_NUMBER(self, token):
        s = str(token)
        return float(s) if '.' in s else int(s)

    
    def ESCAPED_STRING(self, token):
        s = str(token)
        return s[1:-1].encode('utf-8').decode('unicode_escape')

    def CNAME(self, token):
        return str(token)

    def _as_str(self, v):
        from lark import Token
        if isinstance(v, Token):
            return str(v)
        return v

    def _as_number(self, v):
        from lark import Token
        if isinstance(v, Token):
            s = str(v)
            return float(s) if '.' in s else int(s)
        return v

    def _unwrap_tree_token(self, v):
        """Versión mejorada que maneja todos los casos de Trees y Tokens."""
        from lark import Tree, Token
        
        if isinstance(v, Tree):
            # Procesar según el tipo de Tree
            if v.data == 'data_type':
                if v.children:
                    return self._unwrap_tree_token(v.children[0])
                return None
            elif v.data == 'string_literal':
                if v.children and isinstance(v.children[0], Token):
                    return self._process_string_token(v.children[0])
            elif v.data in ['index_type', 'key_field']:
                if v.children:
                    return self._unwrap_tree_token(v.children[0])
            
            # Para otros trees, procesar children recursivamente
            if len(v.children) == 1:
                return self._unwrap_tree_token(v.children[0])
            else:
                return [self._unwrap_tree_token(c) for c in v.children]
        
        elif isinstance(v, Token):
            if v.type == "ESCAPED_STRING":
                return self._process_string_token(v)
            elif v.type in ("INT", "SIGNED_NUMBER"):
                return self._to_number(v)
            elif v.type == "CNAME":
                return str(v)
        
        elif isinstance(v, list):
            if len(v) == 1:
                return self._unwrap_tree_token(v[0])
            return [self._unwrap_tree_token(item) for item in v]
        
        return v

    def _process_string_token(self, token):
        """Procesa tokens de string removiendo comillas y escapando."""
        s = str(token)
        if (s.startswith(('"', "'")) and s.endswith(('"', "'"))):
            s = s[1:-1]
        return s.encode('utf-8').decode('unicode_escape')

    def number(self, token):
        # token puede ser Token('SIGNED_NUMBER', '1') o Tree; usar helper
        return self._as_number(token)

    def string(self, token):
        # token es Tree('string_literal', [Token('ESCAPED_STRING', '"text"')]) o Token
        return self._unwrap_tree_token(token)

    def data_type(self, items):
        """Procesa tipos de datos correctamente."""
        print(f"DEBUG data_type input: {items}")
        
        if not items:
            return None
        
        # Si es un Tree de data_type, procesar sus children
        if hasattr(items[0], 'data') and items[0].data == 'data_type':
            return self._unwrap_tree_token(items[0])
        
        # Si es una lista, tomar el primer elemento
        if isinstance(items, list) and len(items) > 0:
            first_item = items[0]
            
            # Si el primer item es un token de tipo
            if isinstance(first_item, Token) and first_item.type == 'CNAME':
                dtype = str(first_item).upper()
                
                # Verificar si hay tamaño (VARCHAR[20])
                if len(items) > 1:
                    size_item = items[1]
                    if isinstance(size_item, Token) and size_item.type == 'INT':
                        size = int(size_item)
                        return (dtype, size)
                
                return dtype
        
        return self._unwrap_tree_token(items[0])

    def SINGLE_QUOTED_STRING(self, token):
        """Procesa strings con comillas simples."""
        s = str(token)
        if s.startswith("'") and s.endswith("'"):
            s = s[1:-1]
        return s.encode('utf-8').decode('unicode_escape')

    def INT(self, token):
        return "INT"

    def VARCHAR(self, token):
        return "VARCHAR"

    def FLOAT(self, token):
        return "FLOAT"

    def DATE(self, token):
        return "DATE"

    def ARRAY(self, token):
        return "ARRAY"
class SQLParser:
    """Parser SQL principal que devuelve ExecutionPlan."""
    
    def __init__(self, grammar: str = GRAMMAR):
        """Inicializa el parser con la gramática."""
        self.parser = Lark(grammar, parser='lalr', transformer=SQLTransformer())
    
    def parse(self, sql_command: str) -> Union[ExecutionPlan, Dict, None]:
        """
        Parsea un comando SQL y devuelve un ExecutionPlan.
        
        Args:
            sql_command: Comando SQL a parsear
            
        Returns:
            ExecutionPlan o diccionario con el plan de ejecución
            
        Raises:
            LarkError: Si hay errores de sintaxis
        """
        try:
            # Limpiar el comando
            sql_command = sql_command.strip()
            if not sql_command:
                return None
            
            # Parsear
            result = self.parser.parse(sql_command)
            
            # Si es un statement_list, extraer el primer statement
            if isinstance(result, dict) and result.get('type') == 'statement_list':
                statements = result.get('statements', [])
                if statements:
                    return statements[0]  # Devolver el primer ExecutionPlan
                return None
            
            return result
            
        except LarkError as e:
            raise LarkError(f"Error de sintaxis SQL: {e}")
        except Exception as e:
            raise Exception(f"Error interno del parser: {e}")
    
    def parse_file(self, filename: str) -> List[ExecutionPlan]:
        """
        Parsea un archivo con comandos SQL.
        
        Args:
            filename: Ruta del archivo SQL
            
        Returns:
            Lista de ExecutionPlan
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Dividir por ';' y procesar cada comando
        commands = [cmd.strip() for cmd in content.split(';') if cmd.strip()]
        
        plans = []
        for command in commands:
            if not command.startswith('--'):  # Ignorar comentarios
                plan = self.parse(command)
                if plan:
                    if isinstance(plan, dict) and plan.get('type') == 'statement_list':
                        plans.extend(plan['statements'])
                    else:
                        plans.append(plan)
        
        return plans
    
    def parse_file_content(self, content: str) -> List[ExecutionPlan]:
        """
        Parsea contenido de string con comandos SQL.
        
        Args:
            content: Contenido con comandos SQL separados por ';'
            
        Returns:
            Lista de ExecutionPlan
        """
        # Dividir por ';' y procesar cada comando
        commands = [cmd.strip() for cmd in content.split(';') if cmd.strip()]
        
        plans = []
        for command in commands:
            # Ignorar comentarios
            if not command.startswith('--') and not command.startswith('/*'):
                plan = self.parse(command)
                if plan:
                    if isinstance(plan, dict) and plan.get('type') == 'statement_list':
                        plans.extend(plan['statements'])
                    else:
                        plans.append(plan)
        
        return plans

def main():
    """Función principal para testing del parser."""
    parser = SQLParser()
    
    print("=== SQL Parser - Modo Testing ===")
    print("Escriba comandos SQL para ver el ExecutionPlan generado")
    print("Escriba 'exit' para salir")
    print()
    
    while True:
        try:
            command = input("SQL> ").strip()
            if command.lower() == 'exit':
                break
            
            if not command:
                continue
            
            plan = parser.parse(command)
            print(f"ExecutionPlan: {plan}")
            print()
            
        except KeyboardInterrupt:
            print("\nSaliendo...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
