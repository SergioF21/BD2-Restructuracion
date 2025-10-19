#!/usr/bin/env python3
"""
Tests unitarios para el parser SQL (solo parser, sin ejecución).
"""

import unittest
from sql_parser import SQLParser, ExecutionPlan
from lark.exceptions import LarkError

class TestSQLParser(unittest.TestCase):
    """Tests unitarios para el parser SQL."""
    
    def setUp(self):
        """Configuración inicial para cada test."""
        self.parser = SQLParser()
    
    def test_create_table_schema(self):
        """Test CREATE TABLE con esquema."""
        sql = """
        CREATE TABLE Restaurantes (
            id INT KEY INDEX SEQ,
            nombre VARCHAR[20] INDEX BTree,
            fechaRegistro DATE,
            ubicacion ARRAY[FLOAT] INDEX RTree
        )
        """
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'CREATE_TABLE')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        self.assertIsNone(plan.data['source'])
        self.assertEqual(len(plan.data['fields']), 4)
        
        # Verificar campos
        fields = plan.data['fields']
        self.assertEqual(fields[0]['name'], 'id')
        self.assertEqual(fields[0]['type'], 'INT')
        self.assertEqual(fields[0]['index'], 'SEQ')
        
        self.assertEqual(fields[1]['name'], 'nombre')
        self.assertEqual(fields[1]['type'], 'VARCHAR')
        self.assertEqual(fields[1]['size'], 20)
        self.assertEqual(fields[1]['index'], 'BTree')
    
    def test_create_table_from_file(self):
        """Test CREATE TABLE FROM FILE."""
        sql = 'CREATE TABLE Restaurantes FROM FILE "restaurantes.csv" USING INDEX BTree("id")'
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'CREATE_TABLE')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        self.assertEqual(plan.data['source'], 'restaurantes.csv')
        self.assertEqual(plan.data['index_type'], 'BTree')
        self.assertEqual(plan.data['key_field'], 'id')
    
    def test_select_all(self):
        """Test SELECT * FROM table."""
        sql = "SELECT * FROM Restaurantes"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'SELECT')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        self.assertEqual(plan.data['select_list'], '*')
        self.assertIsNone(plan.data['where_clause'])
    
    def test_select_with_where_equals(self):
        """Test SELECT con WHERE field = value."""
        sql = "SELECT * FROM Restaurantes WHERE id = 5"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'SELECT')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        
        where_clause = plan.data['where_clause']
        self.assertEqual(where_clause['type'], 'comparison')
        self.assertEqual(where_clause['field'], 'id')
        self.assertEqual(where_clause['operator'], '=')
        self.assertEqual(where_clause['value'], 5)
    
    def test_select_with_where_between(self):
        """Test SELECT con WHERE field BETWEEN values."""
        sql = "SELECT * FROM Restaurantes WHERE precio BETWEEN 10.5 AND 50.0"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        where_clause = plan.data['where_clause']
        self.assertEqual(where_clause['type'], 'between')
        self.assertEqual(where_clause['field'], 'precio')
        self.assertEqual(where_clause['start'], 10.5)
        self.assertEqual(where_clause['end'], 50.0)
    
    def test_select_with_spatial_condition(self):
        """Test SELECT con condición espacial."""
        sql = "SELECT * FROM Restaurantes WHERE ubicacion IN ((40.4168, -3.7038), 0.1)"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        where_clause = plan.data['where_clause']
        self.assertEqual(where_clause['type'], 'spatial')
        self.assertEqual(where_clause['field'], 'ubicacion')
        self.assertEqual(where_clause['point'], (40.4168, -3.7038))
        self.assertEqual(where_clause['radius'], 0.1)
    
    def test_insert_statement(self):
        """Test INSERT INTO VALUES."""
        sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurante Test", 25.50)'
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'INSERT')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        self.assertEqual(plan.data['values'], [1, "Restaurante Test", 25.50])
    
    def test_delete_statement(self):
        """Test DELETE FROM WHERE."""
        sql = "DELETE FROM Restaurantes WHERE id = 10"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'DELETE')
        self.assertEqual(plan.data['table_name'], 'Restaurantes')
        
        where_clause = plan.data['where_clause']
        self.assertEqual(where_clause['type'], 'comparison')
        self.assertEqual(where_clause['field'], 'id')
        self.assertEqual(where_clause['value'], 10)
    
    def test_string_literals_double_quotes(self):
        """Test strings con comillas dobles."""
        sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurante con comillas dobles", 25.50)'
        
        plan = self.parser.parse(sql)
        values = plan.data['values']
        
        self.assertEqual(values[1], "Restaurante con comillas dobles")
    
    def test_string_literals_single_quotes(self):
        """Test strings con comillas simples."""
        sql = "INSERT INTO Restaurantes VALUES (1, 'Restaurante con comillas simples', 25.50)"
        
        plan = self.parser.parse(sql)
        values = plan.data['values']
        
        self.assertEqual(values[1], "Restaurante con comillas simples")
    
    def test_string_literals_with_escapes(self):
        """Test strings con caracteres escapados."""
        sql = 'INSERT INTO Restaurantes VALUES (1, "Restaurante con \\"comillas\\" escapadas", 25.50)'
        
        plan = self.parser.parse(sql)
        values = plan.data['values']
        
        self.assertEqual(values[1], 'Restaurante con "comillas" escapadas')
    
    def test_numeric_values(self):
        """Test valores numéricos."""
        sql = "INSERT INTO Restaurantes VALUES (1, 'Test', 25.50, -10, 3.14159)"
        
        plan = self.parser.parse(sql)
        values = plan.data['values']
        
        self.assertEqual(values[0], 1)      # int
        self.assertEqual(values[2], 25.50)  # float
        self.assertEqual(values[3], -10)    # negative int
        self.assertEqual(values[4], 3.14159)  # float
    
    def test_point_coordinates(self):
        """Test coordenadas de punto."""
        sql = "SELECT * FROM Restaurantes WHERE ubicacion IN ((40.4168, -3.7038), 0.1)"
        
        plan = self.parser.parse(sql)
        where_clause = plan.data['where_clause']
        
        self.assertEqual(where_clause['point'], (40.4168, -3.7038))
        self.assertEqual(where_clause['radius'], 0.1)
    
    def test_multiple_statements(self):
        """Test múltiples statements."""
        sql = """
        CREATE TABLE Test1 (id INT);
        CREATE TABLE Test2 (id INT);
        SELECT * FROM Test1;
        """
        
        plans = self.parser.parse_file_content(sql)
        
        self.assertEqual(len(plans), 3)
        self.assertEqual(plans[0].operation, 'CREATE_TABLE')
        self.assertEqual(plans[1].operation, 'CREATE_TABLE')
        self.assertEqual(plans[2].operation, 'SELECT')
    
    def test_case_insensitive_keywords(self):
        """Test que las palabras clave sean case-insensitive."""
        sql = "select * from restaurantes where id = 5"
        
        plan = self.parser.parse(sql)
        
        self.assertIsInstance(plan, ExecutionPlan)
        self.assertEqual(plan.operation, 'SELECT')
    
    def test_invalid_syntax(self):
        """Test sintaxis inválida."""
        sql = "SELECT * FROM"  # Sintaxis incompleta
        
        with self.assertRaises(LarkError):
            self.parser.parse(sql)
    
    def test_empty_statement(self):
        """Test statement vacío."""
        result = self.parser.parse("")
        self.assertIsNone(result)
        
        result = self.parser.parse("   ")
        self.assertIsNone(result)
    
    def test_comments(self):
        """Test que los comentarios se ignoren."""
        sql = """
        -- Este es un comentario
        CREATE TABLE Test (id INT);
        /* Este es otro comentario */
        SELECT * FROM Test;
        """
        
        plans = self.parser.parse_file_content(sql)
        
        # Solo debería parsear 2 statements (CREATE y SELECT)
        self.assertEqual(len(plans), 2)
        self.assertEqual(plans[0].operation, 'CREATE_TABLE')
        self.assertEqual(plans[1].operation, 'SELECT')

class TestSQLParserIntegration(unittest.TestCase):
    """Tests de integración para el parser."""
    
    def setUp(self):
        self.parser = SQLParser()
    
    def test_complex_query(self):
        """Test consulta compleja."""
        sql = """
        CREATE TABLE Restaurantes (
            id INT KEY INDEX SEQ,
            nombre VARCHAR[50] INDEX BTree,
            precio FLOAT,
            ubicacion ARRAY[FLOAT] INDEX RTree
        );
        
        INSERT INTO Restaurantes VALUES (1, 'Restaurante Centro', 25.50, (40.4168, -3.7038));
        
        SELECT * FROM Restaurantes WHERE precio BETWEEN 20.0 AND 30.0;
        
        DELETE FROM Restaurantes WHERE id = 1;
        """
        
        plans = self.parser.parse_file_content(sql)
        
        self.assertEqual(len(plans), 4)
        self.assertEqual(plans[0].operation, 'CREATE_TABLE')
        self.assertEqual(plans[1].operation, 'INSERT')
        self.assertEqual(plans[2].operation, 'SELECT')
        self.assertEqual(plans[3].operation, 'DELETE')

def run_parser_tests():
    """Ejecuta todos los tests del parser."""
    print("Ejecutando tests unitarios del parser SQL...")
    
    # Crear suite de tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Agregar tests
    suite.addTests(loader.loadTestsFromTestCase(TestSQLParser))
    suite.addTests(loader.loadTestsFromTestCase(TestSQLParserIntegration))
    
    # Ejecutar tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Mostrar resumen
    print(f"\nResumen de tests:")
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
    run_parser_tests()

