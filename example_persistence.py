#!/usr/bin/env python3
"""
Ejemplo de uso del sistema de persistencia para índices B+ y registros.
"""

from core.models import Table, Field, Record
from core.databasemanager import DatabaseManager

def main():
    # Definir la estructura de la tabla
    fields = [
        Field("id", int),
        Field("nombre", str, 20),
        Field("edad", int),
        Field("ciudad", str, 15)
    ]
    
    table = Table("personas", fields, "id")
    
    # Crear el gestor de base de datos con persistencia
    db = DatabaseManager(table, "personas.dat", order=3)
    
    print("=== Sistema de Base de Datos con Persistencia B+ ===")
    print(f"Información del índice: {db.get_index_info()}")
    
    # Agregar algunos registros
    print("\n--- Agregando registros ---")
    records_to_add = [
        [1, "Juan Pérez", 25, "Madrid"],
        [2, "María García", 30, "Barcelona"],
        [3, "Carlos López", 22, "Valencia"],
        [4, "Ana Martín", 28, "Sevilla"],
        [5, "Luis Rodríguez", 35, "Madrid"]
    ]
    
    for values in records_to_add:
        record = Record(table, values)
        db.add_record(record)
    
    print(f"\nInformación del índice después de agregar: {db.get_index_info()}")
    
    # Buscar un registro específico
    print("\n--- Buscando registro con ID 3 ---")
    record = db.get_record(3)
    if record:
        print(f"Encontrado: {record.values}")
    else:
        print("Registro no encontrado")
    
    # Búsqueda por rango
    print("\n--- Búsqueda por rango (ID 2 a 4) ---")
    range_records = db.range_search(2, 4)
    for record in range_records:
        print(f"ID {record.key}: {record.values}")
    
    # Actualizar un registro
    print("\n--- Actualizando registro con ID 2 ---")
    new_values = [2, "María García López", 31, "Barcelona"]
    db.update_record(2, new_values)
    
    # Verificar la actualización
    updated_record = db.get_record(2)
    if updated_record:
        print(f"Registro actualizado: {updated_record.values}")
    
    # Eliminar un registro
    print("\n--- Eliminando registro con ID 4 ---")
    db.remove_record(4)
    
    # Verificar que fue eliminado
    deleted_record = db.get_record(4)
    if deleted_record is None:
        print("Registro eliminado correctamente")
    
    print(f"\nInformación final del índice: {db.get_index_info()}")
    
    # Mostrar todos los registros restantes
    print("\n--- Todos los registros restantes ---")
    all_records = db.get_all()
    for record in all_records:
        print(f"ID {record.key}: {record.values}")
    
    # Forzar guardado
    print("\n--- Guardando datos ---")
    db.save_all()
    
    print("\n=== Fin del ejemplo ===")
    print("Los datos han sido guardados en:")
    print(f"- Registros: {db.data_filename}")
    print(f"- Índice B+: {db.index_filename}")

if __name__ == "__main__":
    main()
