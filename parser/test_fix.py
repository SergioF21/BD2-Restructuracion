#!/usr/bin/env python3
"""
Test específico para debuggear DELETE.
"""

from sql_parser import SQLParser
from sql_executor import SQLExecutor

def test_delete_debug():
    parser = SQLParser()
    executor = SQLExecutor()
    
    print("=== DEBUG DELETE ESPECÍFICO ===")
    
    # 1. Crear tabla
    print("\n1. CREATE TABLE:")
    sql = 'CREATE TABLE TestDelete FROM FILE "sample_dataset.csv" USING INDEX BTree("id")'
    plan = parser.parse(sql)
    result = executor.execute(plan)
    print(f"   Result: {result.get('success')}")
    
    # 2. DELETE
    print("\n2. DELETE:")
    sql = "DELETE FROM TestDelete WHERE id = 3"
    plan = parser.parse(sql)
    print(f"   Plan: {plan}")
    print(f"   Plan data: {plan.data if plan else 'None'}")
    
    result = executor.execute(plan)
    print(f"   Success: {result.get('success')}")
    print(f"   Message: {result.get('message')}")
    print(f"   Error: {result.get('error')}")

if __name__ == "__main__":
    test_delete_debug()