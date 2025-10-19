#!/usr/bin/env python3
"""
Executor SQL que toma ExecutionPlan y los ejecuta sobre las estructuras de datos.
"""

import os
import sys
import csv
from typing import Dict, List, Any, Optional, Union
from sql_parser import ExecutionPlan

# Agregar el directorio padre al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bplus import BPlusTree
from ExtendibleHashing import ExtendibleHashing
from isam import ISAMIndex
from core.databasemanager import DatabaseManager
from core.models import Table, Field, Record
from rtree import RTree

class SQLExecutor:
    """Executor que ejecuta ExecutionPlan sobre las estructuras de datos."""
    
    def __init__(self, base_dir: str = "."):
        """Inicializa el executor."""
        self.base_dir = base_dir
        self.tables = {}  # Almacena metadatos de las tablas
        self.structures = {}  # Almacena las estructuras de datos activas
    
    def execute(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """
        Ejecuta un ExecutionPlan.
        
        Args:
            plan: Plan de ejecución a ejecutar
            
        Returns:
            Diccionario con el resultado de la ejecución
        """
        try:
            if plan.operation == 'CREATE_TABLE':
                return self._execute_create_table(plan)
            elif plan.operation == 'SELECT':
                return self._execute_select(plan)
            elif plan.operation == 'INSERT':
                return self._execute_insert(plan)
            elif plan.operation == 'UPDATE':
                return self._execute_update(plan)
            elif plan.operation == 'DELETE':
                return self._execute_delete(plan)
            else:
                return {'error': f'Operación no soportada: {plan.operation}'}
                
        except Exception as e:
            return {'error': f'Error ejecutando {plan.operation}: {str(e)}'}
    
    def _execute_create_table(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Ejecuta CREATE TABLE."""
        table_name = plan.data['table_name']
        
        if plan.data.get('source'):  # CREATE TABLE FROM FILE
            return self._create_table_from_file(table_name, plan)
        else:  # CREATE TABLE con esquema
            return self._create_table_from_schema(table_name, plan)
    
    def _create_table_from_file(self, table_name: str, plan: ExecutionPlan) -> Dict[str, Any]:
        """Crea tabla desde archivo CSV."""
        file_path = plan.data['source']
        index_type = plan.data['index_type'].upper()
        key_field = plan.data['key_field']
        
        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            return {'error': f'Archivo no encontrado: {file_path}'}
        
        try:
            # Leer CSV para inferir esquema
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                first_row = next(reader)
            
            # Crear campos
            fields = []
            for col_name, value in first_row.items():
                if value.isdigit():
                    data_type = int
                    size = 0
                elif value.replace('.', '').replace('-', '').isdigit():
                    data_type = float
                    size = 0
                else:
                    data_type = str
                    size = max(20, len(value))
                
                fields.append(Field(col_name, data_type, size))
            
            table = Table(table_name, fields, key_field)
            
            # Crear estructura de datos
            structure = self._create_structure(table_name, index_type, table)
            
            # Cargar datos
            self._load_data_from_csv(table_name, file_path, table, structure, index_type)
            
            # Guardar metadatos
            self.tables[table_name] = {
                'table': table,
                'index_type': index_type,
                'key_field': key_field,
                'source_file': file_path
            }
            self.structures[table_name] = structure
            
            return {
                'success': True,
                'message': f'Tabla "{table_name}" creada exitosamente desde "{file_path}"',
                'rows_loaded': len(first_row) if hasattr(structure, 'file_size') else 'N/A'
            }
            
        except Exception as e:
            return {'error': f'Error creando tabla desde archivo: {e}'}
    
    def _create_table_from_schema(self, table_name: str, plan: ExecutionPlan) -> Dict[str, Any]:
        """Crea tabla desde esquema definido."""
        fields_data = plan.data['fields']
        
        try:
            # Crear campos
            fields = []
            key_field = None
            
            for field_data in fields_data:
                name = field_data['name']
                data_type = field_data['type']
                size = field_data.get('size', 0)
                index_type = field_data.get('index')
                
                # Convertir tipos
                if data_type in ['INT', 'INTEGER']:
                    type_class = int
                elif data_type in ['VARCHAR', 'STRING']:
                    type_class = str
                elif data_type == 'DATE':
                    type_class = str
                elif data_type in ['FLOAT', 'DOUBLE']:
                    type_class = float
                elif data_type == 'ARRAY[FLOAT]':
                    type_class = list
                else:
                    raise ValueError(f'Tipo de dato no soportado: {data_type}')
                
                field_obj = Field(name, type_class, size)
                fields.append(field_obj)
                
                if index_type == 'SEQ':
                    key_field = name
            
            if not key_field:
                key_field = fields[0].name
            
            table = Table(table_name, fields, key_field)
            
            # Crear estructura de datos (usar el primer índice encontrado)
            index_type = fields_data[0].get('index', 'SEQ') if fields_data else 'SEQ'
            structure = self._create_structure(table_name, index_type, table)
            
            # Guardar metadatos
            self.tables[table_name] = {
                'table': table,
                'index_type': index_type,
                'key_field': key_field,
                'fields': fields_data
            }
            self.structures[table_name] = structure
            
            return {
                'success': True,
                'message': f'Tabla "{table_name}" creada exitosamente con esquema',
                'fields': len(fields),
                'index_type': index_type
            }
            
        except Exception as e:
            return {'error': f'Error creando tabla desde esquema: {e}'}
    
    def _create_structure(self, table_name: str, index_type: str, table: Table):
        """Crea la estructura de datos apropiada."""
        index_type = index_type.upper()
        
        if index_type in ['SEQ', 'SEQUENTIAL']:
            return DatabaseManager(table, f"{table_name}.dat", order=3)
        elif index_type in ['BTREE', 'BTREE']:
            return BPlusTree(order=4, index_filename=f"{table_name}_btree.idx")
        elif index_type == 'ISAM':
            return ISAMIndex(f"{table_name}_isam.dat")
        elif index_type in ['EXTENDIBLEHASH', 'EXTENDIBLEHASH']:
            return ExtendibleHashing(bucketSize=3, index_filename=f"{table_name}_hash.idx")
        elif index_type in ['RTREE', 'RTREE']:
            return RTree()
        else:
            raise ValueError(f'Tipo de índice no soportado: {index_type}')
    
    def _load_data_from_csv(self, table_name: str, file_path: str, table: Table, structure, index_type: str):
        """Carga datos desde CSV a la estructura."""
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                values = []
                for field in table.fields:
                    value = row[field.name]
                    if field.data_type == int:
                        values.append(int(value))
                    elif field.data_type == float:
                        values.append(float(value))
                    else:
                        values.append(str(value))
                
                if index_type in ['SEQ', 'SEQUENTIAL']:
                    record = Record(table, values)
                    structure.add_record(record)
                else:
                    key = values[table.index]
                    value_str = str(values)
                    structure.insert(key, value_str)
    
    def _execute_select(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Ejecuta SELECT."""
        table_name = plan.data['table_name']
        
        if table_name not in self.tables:
            return {'error': f'Tabla "{table_name}" no existe'}
        
        table_info = self.tables[table_name]
        structure = self.structures[table_name]
        index_type = table_info['index_type']
        
        try:
            if plan.data.get('where_clause'):
                results = self._execute_where_clause(structure, plan.data['where_clause'], index_type)
            else:
                results = self._select_all(structure, index_type)
            
            return {
                'success': True,
                'results': results,
                'count': len(results) if isinstance(results, list) else 1
            }
            
        except Exception as e:
            return {'error': f'Error ejecutando SELECT: {e}'}
    
    def _execute_where_clause(self, structure, where_clause, index_type):
        """Ejecuta cláusula WHERE."""
        condition_type = where_clause['type']
        field = where_clause['field']
        
        if condition_type == 'comparison':
            value = where_clause['value']
            operator = where_clause['operator']
            
            if index_type in ['SEQ', 'SEQUENTIAL']:
                if operator == '=':
                    result = structure.get_record(value)
                    return [result.values] if result else []
                else:
                    return [f'Operador {operator} no soportado para índice secuencial']
            else:
                if operator == '=':
                    result = structure.search(value)
                    return [result] if result else []
                else:
                    return [f'Operador {operator} no soportado para índice {index_type}']
        
        elif condition_type == 'between':
            start, end = where_clause['start'], where_clause['end']
            
            if index_type in ['SEQ', 'SEQUENTIAL']:
                results = structure.range_search(start, end)
                return [r.values for r in results]
            else:
                results = structure.range_search(start, end)
                return [r[1] for r in results]
        
        elif condition_type == 'spatial':
            point = where_clause['point']
            radius = where_clause['radius']
            
            if hasattr(structure, 'search_radius'):
                return structure.search_radius(point, radius)
            else:
                return [f'Búsqueda espacial no soportada para índice {index_type}']
        
        return []
    
    def _select_all(self, structure, index_type):
        """Selecciona todos los registros."""
        if index_type in ['SEQ', 'SEQUENTIAL']:
            records = structure.get_all()
            return [r.values for r in records]
        else:
            return ['SELECT * no implementado para este tipo de índice']
    
    def _execute_insert(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Ejecuta INSERT."""
        table_name = plan.data['table_name']
        values = plan.data['values']
        
        if table_name not in self.tables:
            return {'error': f'Tabla "{table_name}" no existe'}
        
        table_info = self.tables[table_name]
        structure = self.structures[table_name]
        table = table_info['table']
        index_type = table_info['index_type']
        
        try:
            if index_type in ['SEQ', 'SEQUENTIAL']:
                record = Record(table, values)
                structure.add_record(record)
                return {'success': True, 'message': f'Registro insertado en "{table_name}"'}
            else:
                key = values[table.index]
                value_str = str(values)
                structure.insert(key, value_str)
                return {'success': True, 'message': f'Registro insertado en "{table_name}"'}
                
        except Exception as e:
            return {'error': f'Error insertando registro: {e}'}
    
    def _execute_update(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Ejecuta UPDATE."""
        table_name = plan.data['table_name']
        assignments = plan.data['assignments']
        where_clause = plan.data.get('where_clause')
        
        if table_name not in self.tables:
            return {'error': f'Tabla "{table_name}" no existe'}
        
        # TODO: Implementar UPDATE
        return {'error': 'UPDATE no implementado aún'}
    
    def _execute_delete(self, plan: ExecutionPlan) -> Dict[str, Any]:
        """Ejecuta DELETE."""
        table_name = plan.data['table_name']
        where_clause = plan.data.get('where_clause')
        
        if table_name not in self.tables:
            return {'error': f'Tabla "{table_name}" no existe'}
        
        table_info = self.tables[table_name]
        structure = self.structures[table_name]
        index_type = table_info['index_type']
        
        try:
            if where_clause and where_clause.get('type') == 'comparison':
                field = where_clause['field']
                value = where_clause['value']
                operator = where_clause['operator']
                
                if operator == '=':
                    if index_type in ['SEQ', 'SEQUENTIAL']:
                        success = structure.remove_record(value)
                        return {
                            'success': success,
                            'message': f'Registro eliminado de "{table_name}"' if success else 'Registro no encontrado'
                        }
                    else:
                        result = structure.delete(value)
                        return {
                            'success': bool(result),
                            'message': f'Registro eliminado de "{table_name}"' if result else 'Registro no encontrado'
                        }
                else:
                    return {'error': f'Operador {operator} no soportado en DELETE'}
            else:
                return {'error': 'DELETE sin WHERE no implementado por seguridad'}
                
        except Exception as e:
            return {'error': f'Error eliminando registro: {e}'}
    
    def list_tables(self) -> Dict[str, Any]:
        """Lista todas las tablas creadas."""
        return {
            'success': True,
            'tables': list(self.tables.keys()),
            'count': len(self.tables)
        }
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Obtiene información de una tabla."""
        if table_name not in self.tables:
            return {'error': f'Tabla "{table_name}" no existe'}
        
        table_info = self.tables[table_name]
        return {
            'success': True,
            'table_name': table_name,
            'index_type': table_info['index_type'],
            'key_field': table_info['key_field'],
            'fields': len(table_info['table'].fields)
        }

