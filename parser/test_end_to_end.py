#!/usr/bin/env python3
"""
Tests end-to-end: Parser + Executor + Estructuras de datos.
"""

import unittest
import tempfile
import os
import csv
from sql_parser import SQLParser
from sql_executor import SQLExecutor

class TestEndToEnd(unittest.TestCase):
    """Tests end-to-end del sistema completo."""
    
    def setUp(self):
        """Configuración inicial para cada test."""
        self.parser = SQLParser()
        self.executor = SQLExecutor()
        
        # Crear archivo CSV temporal
        self.temp_csv = self._create_temp_csv()
    
    def tearDown(self):
        """Limpieza después de cada test."""
        if os.path.exists(self.temp_csv):
            os.unlink(self.temp_csv)
    
    def _create_temp_csv(self):
        """Crea un archivo CSV temporal para testing."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        
        # Datos de prueba
        test_data = [
            ["id", "nombre", "precio", "categoria"],
            [1, "Restaurante A", "25.50", "Italiana"],
            [2, "Restaurante B", "35.75", "Mexicana"],
            [3, "Restaurante C", "18.25", "China"],
            [4, "Restaurante D", "42.00", "Francesa"],
            [5, "Restaurante E", "28.90", "Japonesa"]
        ]
        
        writer = csv.writer(temp_file)
        for row in test_data:
            writer.writerow(row)
        
        temp_file.close()
        return temp_file.name
    
    def test_create_table_from_file_btree(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test CREATE TABLE FROM FILE con B+ Tree."""
        sql = f'CREATE TABLE Restaurantes FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        
        # Parsear
        plan = self.parser.parse(sql)
        self.assertIsNotNone(plan)
        
        # Ejecutar
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('Restaurantes', self.executor.tables)
        self.assertEqual(self.executor.tables['Restaurantes']['index_type'], 'BTREE')
    
    def test_create_table_from_file_extendible_hash(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test CREATE TABLE FROM FILE con Extendible Hashing."""
        sql = f'CREATE TABLE RestaurantesHash FROM FILE "{self.temp_csv}" USING INDEX ExtendibleHash("id")'
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('RestaurantesHash', self.executor.tables)
        self.assertEqual(self.executor.tables['RestaurantesHash']['index_type'], 'EXTENDIBLEHASH')
    
    def test_create_table_from_file_isam(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test CREATE TABLE FROM FILE con ISAM."""
        sql = f'CREATE TABLE RestaurantesISAM FROM FILE "{self.temp_csv}" USING INDEX ISAM("id")'
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('RestaurantesISAM', self.executor.tables)
        self.assertEqual(self.executor.tables['RestaurantesISAM']['index_type'], 'ISAM')
    
    def test_create_table_from_file_sequential(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test CREATE TABLE FROM FILE con Sequential File."""
        sql = f'CREATE TABLE RestaurantesSeq FROM FILE "{self.temp_csv}" USING INDEX SEQ("id")'
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('RestaurantesSeq', self.executor.tables)
        self.assertEqual(self.executor.tables['RestaurantesSeq']['index_type'], 'SEQ')
    
    def test_select_all(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test SELECT * FROM table."""
        # Crear tabla primero
        sql = f'CREATE TABLE TestSelect FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # SELECT *
        sql = "SELECT * FROM TestSelect"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertGreater(result['count'], 0)
        self.assertIsInstance(result['results'], list)
    
    def test_select_with_where_equals(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test SELECT con WHERE field = value."""
        # Crear tabla primero - CORREGIR ESTA LÍNEA
        sql = f'CREATE TABLE TestSelect FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'  # QUITAR "WHERE"
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # SELECT con WHERE
        sql = "SELECT * FROM TestSelect WHERE id = 3"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertGreaterEqual(result['count'], 0)
    
    def test_select_with_where_between(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test SELECT con WHERE BETWEEN."""
        # Crear tabla
        sql = f'CREATE TABLE TestBetween FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # SELECT con BETWEEN
        sql = "SELECT * FROM TestBetween WHERE id BETWEEN 2 AND 4"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertGreaterEqual(result['count'], 0)
    
    def test_insert_statement(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test INSERT INTO VALUES."""
        # Crear tabla
        sql = f'CREATE TABLE TestInsert FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # INSERT
        sql = 'INSERT INTO TestInsert VALUES (100, "Nuevo Restaurante", 30.50, "Fusion")'
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('insertado', result['message'])
    
    def test_delete_statement(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test DELETE FROM WHERE."""
        # Crear tabla
        sql = f'CREATE TABLE TestDelete FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # DELETE
        sql = "DELETE FROM TestDelete WHERE id = 3"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('eliminado', result['message'])
    
    def test_create_table_from_schema(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test CREATE TABLE con esquema definido."""
        sql = """
        CREATE TABLE RestaurantesSchema (
            id INT KEY INDEX SEQ,
            nombre VARCHAR[50] INDEX BTree,
            precio FLOAT,
            categoria VARCHAR[20]
        )
        """
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertTrue(result['success'])
        self.assertIn('RestaurantesSchema', self.executor.tables)
        self.assertEqual(self.executor.tables['RestaurantesSchema']['index_type'], 'SEQ')
    
    def test_multiple_operations_sequence(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test secuencia completa de operaciones."""
        # 1. Crear tabla
        sql = f'CREATE TABLE TestSequence FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        self.assertTrue(result['success'])
        
        # 2. SELECT inicial
        sql = "SELECT * FROM TestSequence WHERE id = 1"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        self.assertTrue(result['success'])
        
        # 3. INSERT nuevo registro
        sql = 'INSERT INTO TestSequence VALUES (100, "Restaurante Test", 25.00, "Test")'
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        self.assertTrue(result['success'])
        
        # 4. SELECT el nuevo registro
        sql = "SELECT * FROM TestSequence WHERE id = 100"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        self.assertTrue(result['success'])
        
        # 5. DELETE el registro
        sql = "DELETE FROM TestSequence WHERE id = 100"
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        self.assertTrue(result['success'])
    
    def test_error_handling_table_not_exists(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test manejo de errores - tabla inexistente."""
        sql = "SELECT * FROM TablaInexistente"
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertFalse(result['success'])
        self.assertIn('error', result)
        self.assertIn('no existe', result['error'])
    
    def test_error_handling_invalid_file(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test manejo de errores - archivo inexistente."""
        sql = 'CREATE TABLE TestError FROM FILE "archivo_inexistente.csv" USING INDEX BTree("id")'
        
        plan = self.parser.parse(sql)
        result = self.executor.execute(plan)
        
        self.assertFalse(result['success'])
        self.assertIn('error', result)
        self.assertIn('no encontrado', result['error'])
    
    def test_list_tables(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test listar tablas."""
        # Crear algunas tablas
        tables = ['Test1', 'Test2', 'Test3']
        for table in tables:
            sql = f'CREATE TABLE {table} FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
            plan = self.parser.parse(sql)
            self.executor.execute(plan)
        
        # Listar tablas
        result = self.executor.list_tables()
        
        self.assertTrue(result['success'])
        self.assertEqual(result['count'], len(tables))
        for table in tables:
            self.assertIn(table, result['tables'])
    
    def test_get_table_info(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test obtener información de tabla."""
        # Crear tabla
        sql = f'CREATE TABLE TestInfo FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # Obtener info
        result = self.executor.get_table_info('TestInfo')
        
        self.assertTrue(result['success'])
        self.assertEqual(result['table_name'], 'TestInfo')
        self.assertEqual(result['index_type'], 'BTREE')
        self.assertEqual(result['key_field'], 'id')
        self.assertGreater(result['fields'], 0)

class TestEndToEndIntegration(unittest.TestCase):
    """Tests de integración más complejos."""
    
    def setUp(self):
        self.parser = SQLParser()
        self.executor = SQLExecutor()
        self.temp_csv = self._create_temp_csv()
    
    def tearDown(self):
        if os.path.exists(self.temp_csv):
            os.unlink(self.temp_csv)
    
    def _create_temp_csv(self):
        """Crea un archivo CSV temporal para testing con ruta segura."""
        import tempfile
        import os
        
        # Usar tempfile para ruta segura
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, f"test_data_{os.getpid()}.csv")
        
        # Datos de prueba
        test_data = [
            ["id", "nombre", "precio", "categoria"],
            [1, "Restaurante A", "25.50", "Italiana"],
            [2, "Restaurante B", "35.75", "Mexicana"],
            [3, "Restaurante C", "18.25", "China"],
            [4, "Restaurante D", "42.00", "Francesa"],
            [5, "Restaurante E", "28.90", "Japonesa"]
        ]
        
        with open(temp_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for row in test_data:
                writer.writerow(row)
        
        return temp_file
    
    def test_multiple_index_types_same_data(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test crear múltiples tablas con diferentes índices para los mismos datos."""
        index_types = ['BTree', 'ExtendibleHash', 'ISAM', 'SEQ']
        
        for index_type in index_types:
            table_name = f'Test{index_type}'
            sql = f'CREATE TABLE {table_name} FROM FILE "{self.temp_csv}" USING INDEX {index_type}("id")'
            
            plan = self.parser.parse(sql)
            result = self.executor.execute(plan)
            
            self.assertTrue(result['success'], f'Error creando tabla con índice {index_type}')
            self.assertIn(table_name, self.executor.tables)
        
        # Verificar que todas las tablas existen
        list_result = self.executor.list_tables()
        self.assertEqual(list_result['count'], len(index_types))
    
    def test_complex_query_workflow(self):
        print(f"=== Running test: {self._testMethodName} ===")
        """Test flujo de trabajo complejo."""
        # Crear tabla
        sql = f'CREATE TABLE WorkflowTest FROM FILE "{self.temp_csv}" USING INDEX BTree("id")'
        plan = self.parser.parse(sql)
        self.executor.execute(plan)
        
        # Operaciones complejas
        operations = [
            "SELECT * FROM WorkflowTest WHERE id = 1",
            "SELECT * FROM WorkflowTest WHERE id BETWEEN 2 AND 4",
            'INSERT INTO WorkflowTest VALUES (100, "Nuevo", 30.0)',
            "SELECT * FROM WorkflowTest WHERE id = 100",
            "DELETE FROM WorkflowTest WHERE id = 100",
            "SELECT * FROM WorkflowTest"
        ]
        
        for operation in operations:
            plan = self.parser.parse(operation)
            result = self.executor.execute(plan)
            
            self.assertTrue(result['success'], f'Error en operación: {operation}')

def run_end_to_end_tests():
    """Ejecuta todos los tests end-to-end."""
    print("Ejecutando tests end-to-end del sistema SQL...")
    
    # Crear suite de tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar tests
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEnd))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndIntegration))
    
    # Ejecutar tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Mostrar resumen
    print(f"\nResumen de tests end-to-end:")
    print(f"Tests ejecutados: {result.testsRun}")
    print(f"Fallos: {len(result.failures)}")
    print(f"Errores: {len(result.errors)}")
    
    if result.failures:
        print("\nFallos:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nErrores:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    run_end_to_end_tests()

