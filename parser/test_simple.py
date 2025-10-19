#!/usr/bin/env python3
"""
Test simple del parser SQL.
""" 

from sql_parser import SQLParser

def test_simple_parser():
    """Test básico del parser."""
    parser = SQLParser()
    
    print("=== TEST SIMPLE DEL PARSER SQL ===")
    
    # Test 1: CREATE TABLE simple
    print("\n1. Test CREATE TABLE simple:")
    sql1 = "CREATE TABLE Test (id INT, nombre VARCHAR[20])"
    try:
        plan1 = parser.parse(sql1)
        print(f"SQL: {sql1}")
        print(f"Plan: {plan1}")
        print("Éxito")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 2: CREATE TABLE FROM FILE
    print("\n2. Test CREATE TABLE FROM FILE:")
    sql2 = 'CREATE TABLE Test2 FROM FILE "datos.csv" USING INDEX BTree("id")'
    try:
        plan2 = parser.parse(sql2)
        print(f"SQL: {sql2}")
        print(f"Plan: {plan2}")
        print("Éxito")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 3: SELECT simple
    print("\n3. Test SELECT simple:")
    sql3 = "SELECT * FROM Test"
    try:
        plan3 = parser.parse(sql3)
        print(f"SQL: {sql3}")
        print(f"Plan: {plan3}")
        print("Éxito")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 4: INSERT simple
    print("\n4. Test INSERT simple:")
    sql4 = 'INSERT INTO Test VALUES (1, "test")'
    try:
        plan4 = parser.parse(sql4)
        print(f"SQL: {sql4}")
        print(f"Plan: {plan4}")
        print("Éxito")
    except Exception as e:
        print(f"Error: {e}")
    
    # Test 5: DELETE simple
    print("\n5. Test DELETE simple:")
    sql5 = "DELETE FROM Test WHERE id = 1"
    try:
        plan5 = parser.parse(sql5)
        print(f"SQL: {sql5}")
        print(f"Plan: {plan5}")
        print("Éxito")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\nFIN DEL TEST")

if __name__ == "__main__":
    test_simple_parser()

