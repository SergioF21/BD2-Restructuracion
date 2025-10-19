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
        # items: puede contener table name, filename (string token), index_type, key_field (CNAME or string)
        table_name = None
        file_path = None
        index_type = None
        key_field = None
        from lark import Token, Tree

        # items puede venir como lista/Tree/Token/str; iteramos y desempaquetamos
        for it in items:
            if isinstance(it, Token):
                if it.type == "CNAME" and table_name is None:
                    table_name = str(it)
                elif it.type == "ESCAPED_STRING" and file_path is None:
                    file_path = str(it)[1:-1]
            elif isinstance(it, str):
                # si uno de los strings es nombre de archivo o index type o key
                if table_name is None:
                    table_name = it
                elif file_path is None and (it.endswith('.csv') or it.startswith('/') or it.startswith('.')):
                    file_path = it.strip('"').strip("'")
                elif index_type is None:
                    index_type = it
                elif key_field is None:
                    key_field = it
            elif isinstance(it, Tree):
                # key_field puede llegar como Tree('string', [Token('ESCAPED_STRING', '"id"')])
                val = self._unwrap_tree_token(it)
                if isinstance(val, str) and key_field is None:
                    key_field = val
        # limpiar comillas en key_field si vienen
        if isinstance(key_field, str) and (key_field.startswith('"') or key_field.startswith("'")):
            key_field = key_field.strip('"').strip("'")
        if isinstance(index_type, str):
            index_type_norm = index_type.upper()
        else:
            index_type_norm = None
        return ExecutionPlan('CREATE_TABLE', table_name=table_name, fields=None, source=file_path, index_type=index_type_norm, key_field=key_field)

        
    # --- field definition and list ---
    def field_definition(self, *items):
        # Expect: name, data_type (possibly "VARCHAR", size), optional index_option
        name = None
        dtype = None
        size = None
        idx = None
        for it in items:
            if isinstance(it, str) and name is None:
                name = it
            elif isinstance(it, dict) and 'index' in it:
                idx = it['index']
            elif isinstance(it, (int, float)):
                size = int(it)
            elif isinstance(it, str) and dtype is None:
                dtype = it
            elif isinstance(it, list):
                # could be emitted as list, search inside
                for sub in it:
                    if isinstance(sub, str) and dtype is None:
                        dtype = sub
                    if isinstance(sub, int) and size is None:
                        size = int(sub)
        # normalize dtype: if dtype like VARCHAR and size present, create 'VARCHAR[20]'
        if dtype and size:
            dtype_repr = f"{dtype}[{size}]"
        else:
            dtype_repr = dtype
        return {"name": name, "type": dtype_repr, "size": size or 0, "index": idx}

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
    def comparison(self, *items):
        # items: field, operator, value
        if len(items) >= 3:
            field = self._unwrap(items[0])
            op = str(items[1])
            val = self._unwrap(items[2])
            return {"type":"cmp", "field": field, "op": op, "value": val}
        return None

    def condition(self, *items):
        # left-associative combine: items like comp, op, comp, op, comp...
        if not items:
            return None
        node = items[0]
        i = 1
        while i < len(items):
            op_tok = items[i]
            right = items[i+1]
            op = str(op_tok).lower()
            node = {"type": op, "left": node, "right": right}
            i += 2
        return node

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


    def value_list(self, *items):
        return [self._unwrap(i) for i in items]

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
        table = None
        where = None
        from lark import Token
        for it in items:
            if isinstance(it, Token) and it.type == "CNAME" and table is None:
                table = str(it)
            elif isinstance(it, str) and table is None:
                table = it
            elif isinstance(it, dict) and it.get('type') in ('cmp','and','or','between'):
                where = it
        return ExecutionPlan('DELETE', table_name=table, where_clause=where)


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

    def INT(self, tok):
        return int(str(tok))
    
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
        # devuelve value limpio si v es Tree(Token) o Token
        from lark import Tree, Token
        if isinstance(v, Token):
            if v.type in ("INT",):
                return int(str(v))
            if v.type in ("SIGNED_NUMBER",):
                return self._as_number(v)
            if v.type == "ESCAPED_STRING":
                s = str(v); return s[1:-1].encode('utf-8').decode('unicode_escape')
            return str(v)
        if isinstance(v, Tree):
            if len(v.children) == 1:
                return self._unwrap_tree_token(v.children[0])
            return [self._unwrap_tree_token(c) for c in v.children]
        return v

    def number(self, token):
        # token puede ser Token('SIGNED_NUMBER', '1') o Tree; usar helper
        return self._as_number(token)

    def string(self, token):
        # token es Tree('string_literal', [Token('ESCAPED_STRING', '"text"')]) o Token
        return self._unwrap_tree_token(token)

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
