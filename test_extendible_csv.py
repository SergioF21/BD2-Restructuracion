#!/usr/bin/env python3
"""
Script para probar Extendible Hashing específicamente con el dataset CSV.
"""

import csv
from ExtendibleHashing import ExtendibleHashing

def load_csv_data(filename: str = "sample_dataset.csv"):
    """Carga datos del archivo CSV y los devuelve en diferentes formatos."""
    print(f"Cargando datos desde {filename}...")
    
    records = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            records = list(reader)
        
        print(f"{len(records)} registros cargados exitosamente")
        
        # Mostrar muestra de datos
        print("\nMuestra de datos:")
        for i, record in enumerate(records[:5]):
            print(f"  {i+1}. ID: {record['id']}, Producto: {record['product_name']}, Precio: ${record['price']}, Categoría: {record['category']}")
        
        return records
        
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {filename}")
        return []
    except Exception as e:
        print(f"Error al cargar datos: {e}")
        return []

def test_extendible_hashing_with_csv():
    """Prueba completa del Extendible Hashing con datos del CSV."""
    print("=== PRUEBA DE EXTENDIBLE HASHING CON DATOS CSV ===\n")
    
    # Cargar datos del CSV
    records = load_csv_data()
    if not records:
        return
    
    # Crear Extendible Hashing con persistencia
    print("\nCreando Extendible Hashing...")
    eh = ExtendibleHashing(bucketSize=3, index_filename="extendible_csv_test.idx")
    
    # Intentar cargar datos existentes
    if eh.load_from_file():
        print("Datos cargados desde archivo existente")
    else:
        print("Nuevo Extendible Hashing creado")
    
    print(f"Estado inicial: D={eh.D}, Directorio={len(eh.directory)} entradas")
    
    # ========== PRUEBA 1: INSERCIÓN POR ID ==========
    print("\n" + "="*50)
    print("PRUEBA 1: INSERCIÓN POR ID")
    print("="*50)
    
    print("Insertando registros usando ID como clave...")
    inserted_count = 0
    
    for record in records:
        key = int(record['id'])
        value = f"{record['product_name']} - ${record['price']}"
        eh.insert(key, value)
        inserted_count += 1
        
        # Mostrar progreso cada 10 registros
        if inserted_count % 10 == 0:
            print(f"  Progreso: {inserted_count}/{len(records)} registros insertados")
    
    print(f"{inserted_count} registros insertados exitosamente")
    print(f"Estado después de inserción: D={eh.D}, Directorio={len(eh.directory)} entradas")
    
    # ========== PRUEBA 2: BÚSQUEDAS INDIVIDUALES ==========
    print("\n" + "="*50)
    print("PRUEBA 2: BÚSQUEDAS INDIVIDUALES")
    print("="*50)
    
    test_keys = [1, 5, 15, 25, 30]
    
    for key in test_keys:
        result = eh.search(key)
        if result:
            print(f"  ID {key}: {result}")
        else:
            print(f"  ID {key}: No encontrado")
    
    # ========== PRUEBA 3: BÚSQUEDA POR RANGO ==========
    print("\n" + "="*50)
    print("PRUEBA 3: BÚSQUEDA POR RANGO")
    print("="*50)
    
    ranges_to_test = [
        (1, 10, "Primeros 10 productos"),
        (15, 25, "Productos del medio"),
        (25, 30, "Últimos 5 productos")
    ]
    
    for start, end, description in ranges_to_test:
        results = eh.range_search(start, end)
        print(f"  {description} (ID {start}-{end}): {len(results)} productos encontrados")
        
        # Mostrar algunos resultados
        for i, (key, value) in enumerate(results[:3]):
            print(f"    - ID {key}: {value}")
        if len(results) > 3:
            print(f"    ... y {len(results) - 3} más")
    
    # ========== PRUEBA 4: ACTUALIZACIONES ==========
    print("\n" + "="*50)
    print("PRUEBA 4: ACTUALIZACIONES")
    print("="*50)
    
    updates_to_test = [
        (10, "Producto Actualizado - $999.99"),
        (20, "Nuevo Nombre del Producto - $149.99")
    ]
    
    for key, new_value in updates_to_test:
        print(f"  Actualizando ID {key}...")
        result = eh.update(key, new_value)
        print(f"    {result}")
        
        # Verificar la actualización
        updated_result = eh.search(key)
        if updated_result:
            print(f"    Verificado: {updated_result}")
        else:
            print(f"    Error: No se pudo verificar la actualización")
    
    # ========== PRUEBA 5: ELIMINACIONES ==========
    print("\n" + "="*50)
    print("PRUEBA 5: ELIMINACIONES")
    print("="*50)
    
    keys_to_delete = [12, 18, 28]
    
    for key in keys_to_delete:
        print(f"  Eliminando ID {key}...")
        result = eh.delete(key)
        print(f"    {result}")
        
        # Verificar la eliminación
        deleted_result = eh.search(key)
        if deleted_result is None:
            print(f"    Verificado: Producto eliminado correctamente")
        else:
            print(f"    Error: Producto aún existe")
    
    # ========== PRUEBA 6: BÚSQUEDA DESPUÉS DE MODIFICACIONES ==========
    print("\n" + "="*50)
    print("PRUEBA 6: BÚSQUEDA DESPUÉS DE MODIFICACIONES")
    print("="*50)
    
    print("Verificando algunos registros después de las modificaciones...")
    verification_keys = [10, 20, 12, 18, 28, 25]
    
    for key in verification_keys:
        result = eh.search(key)
        if result:
            print(f"  ID {key}: {result}")
        else:
            print(f"  ID {key}: No encontrado (posiblemente eliminado)")
    
    # ========== ESTADÍSTICAS FINALES ==========
    print("\n" + "="*50)
    print("ESTADÍSTICAS FINALES")
    print("="*50)
    
    print(f"Profundidad global (D): {eh.D}")
    print(f"Tamaño del directorio: {len(eh.directory)} entradas")
    print(f"Factor de bloque: {eh.bucketSize}")
    
    # Contar registros únicos en el directorio
    unique_buckets = set()
    for bucket in eh.directory:
        unique_buckets.add(id(bucket))
    
    print(f"Número de buckets únicos: {len(unique_buckets)}")
    
    # ========== GUARDAR Y VERIFICAR PERSISTENCIA ==========
    print("\n" + "="*50)
    print("PRUEBA DE PERSISTENCIA")
    print("="*50)
    
    print("Guardando datos...")
    eh.save_to_file()
    print("Datos guardados en extendible_csv_test.idx")
    
    # Verificar que el archivo se creó
    import os
    if os.path.exists("extendible_csv_test.idx"):
        file_size = os.path.getsize("extendible_csv_test.idx")
        print(f"Archivo creado: {file_size} bytes")
    
    print("\n=== PRUEBA COMPLETADA EXITOSAMENTE ===")
    print("Puedes ejecutar este script nuevamente para ver que los datos se cargan correctamente desde el archivo.")
    
    return eh

def main():
    """Función principal."""
    print("PRUEBA DE EXTENDIBLE HASHING CON DATOS CSV")
    print("Dataset: sample_dataset.csv (30 productos de e-commerce)")
    print("Estructura: Extendible Hashing con persistencia\n")
    
    try:
        eh = test_extendible_hashing_with_csv()
        print(f"\nTodas las pruebas completadas exitosamente")
        
    except Exception as e:
        print(f"\nError durante las pruebas: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
