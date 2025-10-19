#!/usr/bin/env python3
"""
SQL REPL (Read-Eval-Print Loop) con manejo robusto de errores y logging.
"""

import sys
import os
import traceback
from typing import Dict, Any, List
from sql_parser import SQLParser
from sql_executor import SQLExecutor
from lark.exceptions import LarkError

class SQLError(Exception):
    """Excepción personalizada para errores SQL."""
    
    def __init__(self, message: str, position: int = None, line: int = None, column: int = None):
        super().__init__(message)
        self.message = message
        self.position = position
        self.line = line
        self.column = column
    
    def __str__(self):
        if self.line and self.column:
            return f"Error en línea {self.line}, columna {self.column}: {self.message}"
        elif self.position:
            return f"Error en posición {self.position}: {self.message}"
        else:
            return self.message

class SQLLogger:
    """Sistema de logging para el SQL REPL."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.command_count = 0
    
    def log_command(self, command: str):
        """Log de comando ejecutado."""
        self.command_count += 1
        if self.verbose:
            print(f"[CMD {self.command_count}] {command}")
    
    def log_success(self, message: str):
        """Log de operación exitosa."""
        if self.verbose:
            print(f"[OK] {message}")
        else:
            print(message)
    
    def log_error(self, error: Exception):
        """Log de error con detalles."""
        print(f"[ERROR] {error}")
        
        if self.verbose and isinstance(error, SQLError):
            if error.line and error.column:
                print(f"  Posición: línea {error.line}, columna {error.column}")
    
    def log_info(self, message: str):
        """Log de información."""
        if self.verbose:
            print(f"[INFO] {message}")

class SQLREPL:
    """REPL principal para comandos SQL."""
    
    def __init__(self, verbose: bool = False):
        """Inicializa el REPL."""
        self.parser = SQLParser()
        self.executor = SQLExecutor()
        self.logger = SQLLogger(verbose)
        self.verbose = verbose
    
    def execute_command(self, command: str) -> Dict[str, Any]:
        """
        Ejecuta un comando SQL completo.
        
        Args:
            command: Comando SQL a ejecutar
            
        Returns:
            Diccionario con el resultado
        """
        try:
            # Log del comando
            self.logger.log_command(command)
            
            # Parsear comando
            plan = self.parser.parse(command)
            
            if plan is None:
                return {'success': True, 'message': 'Comando vacío'}
            
            # Ejecutar plan
            result = self.executor.execute(plan)
            
            # Log del resultado
            if result.get('success'):
                self.logger.log_success(result.get('message', 'Operación exitosa'))
            else:
                self.logger.log_error(SQLError(result.get('error', 'Error desconocido')))
            
            return result
            
        except LarkError as e:
            error = SQLError(f"Error de sintaxis: {e}")
            self.logger.log_error(error)
            return {'success': False, 'error': str(error)}
            
        except Exception as e:
            error = SQLError(f"Error interno: {e}")
            self.logger.log_error(error)
            
            if self.verbose:
                traceback.print_exc()
            
            return {'success': False, 'error': str(error)}
    
    def execute_file(self, filename: str) -> List[Dict[str, Any]]:
        """
        Ejecuta comandos SQL desde un archivo.
        
        Args:
            filename: Ruta del archivo SQL
            
        Returns:
            Lista de resultados
        """
        if not os.path.exists(filename):
            error = SQLError(f"Archivo no encontrado: {filename}")
            self.logger.log_error(error)
            return [{'success': False, 'error': str(error)}]
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parsear todos los comandos
            plans = self.parser.parse_file_content(content)
            
            results = []
            for i, plan in enumerate(plans):
                self.logger.log_info(f"Ejecutando comando {i+1}/{len(plans)}")
                
                result = self.executor.execute(plan)
                results.append(result)
                
                if result.get('success'):
                    self.logger.log_success(result.get('message', f'Comando {i+1} ejecutado'))
                else:
                    self.logger.log_error(SQLError(result.get('error', f'Error en comando {i+1}')))
            
            return results
            
        except Exception as e:
            error = SQLError(f"Error procesando archivo: {e}")
            self.logger.log_error(error)
            return [{'success': False, 'error': str(error)}]
    
    def show_help(self):
        """Muestra la ayuda del sistema."""
        help_text = """
=== SQL REPL - Sistema de Base de Datos ===

Comandos SQL soportados:

1. CREATE TABLE:
   - CREATE TABLE nombre (campo1 TIPO, campo2 TIPO INDEX INDICE);
   - CREATE TABLE nombre FROM FILE "archivo.csv" USING INDEX tipo("campo");

2. SELECT:
   - SELECT * FROM tabla;
   - SELECT * FROM tabla WHERE campo = valor;
   - SELECT * FROM tabla WHERE campo BETWEEN inicio AND fin;

3. INSERT:
   - INSERT INTO tabla VALUES (valor1, valor2, ...);

4. DELETE:
   - DELETE FROM tabla WHERE campo = valor;

5. Comandos especiales:
   - .help - Mostrar esta ayuda
   - .tables - Listar tablas
   - .info tabla - Información de tabla
   - .verbose - Activar/desactivar modo verbose
   - .exit - Salir

Tipos de índices soportados:
- SEQ/SEQUENTIAL: Archivo secuencial
- BTree: Árbol B+
- ExtendibleHash: Hashing extensible
- ISAM: Índice secuencial de acceso múltiple
- RTree: Árbol R para datos espaciales

Ejemplos:
CREATE TABLE Restaurantes FROM FILE "datos.csv" USING INDEX BTree("id");
SELECT * FROM Restaurantes WHERE precio BETWEEN 20 AND 50;
INSERT INTO Restaurantes VALUES (100, "Nuevo", 25.50);
DELETE FROM Restaurantes WHERE id = 100;
        """
        print(help_text)
    
    def show_tables(self):
        """Muestra las tablas creadas."""
        result = self.executor.list_tables()
        if result['success']:
            if result['tables']:
                print(f"\nTablas creadas ({result['count']}):")
                for table in result['tables']:
                    print(f"  - {table}")
            else:
                print("\nNo hay tablas creadas.")
        else:
            print(f"\nError: {result['error']}")
    
    def show_table_info(self, table_name: str):
        """Muestra información de una tabla."""
        result = self.executor.get_table_info(table_name)
        if result['success']:
            print(f"\nInformación de la tabla '{table_name}':")
            print(f"  Tipo de índice: {result['index_type']}")
            print(f"  Campo clave: {result['key_field']}")
            print(f"  Número de campos: {result['fields']}")
        else:
            print(f"\nError: {result['error']}")
    
    def run_interactive(self):
        """Ejecuta el REPL en modo interactivo."""
        print("=== SQL REPL - Sistema de Base de Datos ===")
        print("Escriba comandos SQL o '.help' para ayuda")
        print("Escriba '.exit' para salir")
        print()
        
        while True:
            try:
                # Leer comando
                command = input("SQL> ").strip()
                
                if not command:
                    continue
                
                # Comandos especiales
                if command.lower() == '.exit':
                    print("¡Hasta luego!")
                    break
                elif command.lower() == '.help':
                    self.show_help()
                    continue
                elif command.lower() == '.tables':
                    self.show_tables()
                    continue
                elif command.lower().startswith('.info '):
                    table_name = command[6:].strip()
                    self.show_table_info(table_name)
                    continue
                elif command.lower() == '.verbose':
                    self.verbose = not self.verbose
                    self.logger.verbose = self.verbose
                    print(f"Modo verbose: {'activado' if self.verbose else 'desactivado'}")
                    continue
                
                # Ejecutar comando SQL
                result = self.execute_command(command)
                
                # Mostrar resultado detallado en modo verbose
                if self.verbose and result.get('success'):
                    if 'results' in result:
                        print(f"Resultados ({result.get('count', 0)} registros):")
                        for i, row in enumerate(result['results'][:5]):  # Mostrar solo los primeros 5
                            print(f"  {i+1}: {row}")
                        if result.get('count', 0) > 5:
                            print(f"  ... y {result.get('count', 0) - 5} más")
                
                print()  # Línea en blanco
                
            except KeyboardInterrupt:
                print("\n\nSaliendo...")
                break
            except EOFError:
                print("\n\nSaliendo...")
                break
            except Exception as e:
                print(f"\nError inesperado: {e}")
                if self.verbose:
                    traceback.print_exc()

def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='SQL REPL con múltiples estructuras de datos')
    parser.add_argument('-v', '--verbose', action='store_true', help='Modo verbose')
    parser.add_argument('-f', '--file', help='Ejecutar archivo SQL')
    
    args = parser.parse_args()
    
    repl = SQLREPL(verbose=args.verbose)
    
    if args.file:
        # Ejecutar archivo
        results = repl.execute_file(args.file)
        
        # Mostrar resumen
        success_count = sum(1 for r in results if r.get('success'))
        total_count = len(results)
        
        print(f"\nResumen: {success_count}/{total_count} comandos ejecutados exitosamente")
        
        if success_count < total_count:
            print("Comandos con errores:")
            for i, result in enumerate(results):
                if not result.get('success'):
                    print(f"  {i+1}: {result.get('error', 'Error desconocido')}")
    else:
        # Modo interactivo
        repl.run_interactive()

if __name__ == "__main__":
    main()

