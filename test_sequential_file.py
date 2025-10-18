import os
import time
import random
from core.models import Table, Field, Record
from sequential_file import SequentialIndex

def generate_test_data(filename: str, num_records: int):
    """Genera datos de prueba aleatorios"""
    names = ["Juan", "Maria", "Carlos", "Ana", "Pedro", "Sofia", "Miguel", "Laura", "Diego", "Carmen"]
    lastnames = ["Garcia", "Martinez", "Lopez", "Gonzalez", "Rodriguez", "Fernandez", "Perez", "Sanchez"]
    
    # Crear tabla de prueba
    table = Table("usuarios", [
        Field("id", int),
        Field("nombre", str, 50),
        Field("edad", int)
    ], "id")
    
    records = []
    used_ids = set()
    
    for _ in range(num_records):
        # Generar ID único
        while True:
            record_id = random.randint(1000, 9999)
            if record_id not in used_ids:
                used_ids.add(record_id)
                break
        
        name = f"{random.choice(names)} {random.choice(lastnames)}"
        age = random.randint(18, 80)
        
        record = Record(table, [record_id, name, age])
        records.append(record)
    
    # Ordenar por ID para simular un archivo inicial ordenado
    records.sort(key=lambda r: r.key)
    
    # Escribir al archivo
    with open(filename, 'wb') as f:
        for record in records:
            f.write(record.pack())
    
    print(f"✅ Generados {num_records} registros aleatorios en {filename}")
    return table, records

def show_file_stats(sf: SequentialIndex):
    """Muestra estadísticas de los archivos"""
    print("\n" + "="*50)
    print("📊 ESTADÍSTICAS DEL ARCHIVO")
    print("="*50)
    
    main_size = 0
    if os.path.exists(sf.data_filename):
        main_size = os.path.getsize(sf.data_filename) // sf.record_size
    
    aux_size = sf.aux_records_count
    
    print(f"Registros en archivo principal: {main_size}")
    print(f"Registros en archivo auxiliar: {aux_size}")
    print(f"Total de registros: {main_size + aux_size}")
    print(f"Umbral K: {sf.K_threshold}")
    print(f"Estado: {'Necesita reconstrucción' if aux_size >= sf.K_threshold else 'Normal'}")
    print("="*50)

def display_sample_records(sf: SequentialIndex, count: int = 5):
    """Muestra algunos registros del archivo principal"""
    print(f"\n📋 PRIMEROS {count} REGISTROS DEL ARCHIVO PRINCIPAL")
    print("-" * 60)
    
    try:
        with open(sf.data_filename, 'rb') as f:
            displayed = 0
            while displayed < count:
                data = f.read(sf.record_size)
                if not data:
                    break
                
                record = Record.unpack(sf.table, data)
                if record.next == 0:  # No eliminado
                    print(f"ID: {record.key:4d} | Nombre: {record.values[1]:20s} | Edad: {record.values[2]:2d}")
                    displayed += 1
    except FileNotFoundError:
        print("Archivo principal no existe")
    
    print("-" * 60)

def test_insert(sf: SequentialIndex):
    """Prueba de inserción"""
    print("\n🔸 PRUEBA DE INSERCIÓN")
    print("=" * 40)
    
    test_records = [
        [1500, "Test Usuario1", 25],
        [2500, "Test Usuario2", 30], 
        [3500, "Test Usuario3", 35],
        [500, "Test Usuario4", 40],   # Menor que muchos
        [9500, "Test Usuario5", 45],  # Mayor que muchos
    ]
    
    start_time = time.time()
    
    for values in test_records:
        record = Record(sf.table, values)
        sf.add(record)
        print(f"✅ Insertado: ID={values[0]}, Nombre={values[1]}")
        show_file_stats(sf)
    
    end_time = time.time()
    print(f"⏱️ Tiempo total de inserción: {(end_time - start_time)*1000:.2f} ms")

def test_search(sf: SequentialIndex):
    """Prueba de búsqueda"""
    print("\n🔍 PRUEBA DE BÚSQUEDA")
    print("=" * 40)
    
    search_ids = [1500, 2500, 9999, 1, 5000, 500, 9500]
    
    for search_id in search_ids:
        start_time = time.time()
        result = sf.search(search_id)
        end_time = time.time()
        
        duration_ms = (end_time - start_time) * 1000
        
        if result:
            print(f"✅ ID {search_id:4d}: {result.values[1]:20s} | Edad: {result.values[2]:2d} ({duration_ms:.3f} ms)")
        else:
            print(f"❌ ID {search_id:4d}: No encontrado ({duration_ms:.3f} ms)")

def test_range_search(sf: SequentialIndex):
    """Prueba de búsqueda por rango"""
    print("\n📊 PRUEBA DE BÚSQUEDA POR RANGO")
    print("=" * 40)
    
    ranges = [
        (1000, 2000, "Rango bajo (1000-2000)"),
        (2000, 3000, "Rango medio (2000-3000)"),
        (8000, 9999, "Rango alto (8000-9999)"),
        (500, 1000, "Rango pequeño (500-1000)"),
        (1, 10000, "Rango completo (1-10000)")
    ]
    
    for begin_key, end_key, description in ranges:
        start_time = time.time()
        results = sf.rangeSearch(begin_key, end_key)
        end_time = time.time()
        
        duration_ms = (end_time - start_time) * 1000
        
        print(f"\n{description}:")
        print(f"  📈 Encontrados: {len(results)} registros ({duration_ms:.3f} ms)")
        
        if results:
            print("  🔹 Primeros resultados:")
            for i, record in enumerate(results[:3]):
                print(f"    ID: {record.key:4d} | {record.values[1]:20s} | Edad: {record.values[2]:2d}")
            if len(results) > 3:
                print(f"    ... y {len(results) - 3} más")

def test_delete(sf: SequentialIndex):
    """Prueba de eliminación"""
    print("\n🗑️ PRUEBA DE ELIMINACIÓN")
    print("=" * 40)
    
    delete_ids = [1500, 9999, 2500, 1, 500]  # Algunos existen, otros no
    
    for delete_id in delete_ids:
        start_time = time.time()
        result = sf.remove(delete_id)
        end_time = time.time()
        
        duration_ms = (end_time - start_time) * 1000
        
        if result:
            print(f"✅ Eliminado ID {delete_id:4d} ({duration_ms:.3f} ms)")
        else:
            print(f"❌ No se pudo eliminar ID {delete_id:4d} ({duration_ms:.3f} ms)")
    
    show_file_stats(sf)
    
    # Verificar que los eliminados ya no se encuentran
    print("\n🔍 Verificando eliminaciones:")
    for delete_id in delete_ids:
        result = sf.search(delete_id)
        status = "❌ Aún existe" if result else "✅ Eliminado correctamente"
        print(f"  ID {delete_id:4d}: {status}")

def test_rebuild(sf: SequentialIndex):
    """Prueba de reconstrucción"""
    print("\n🔄 PRUEBA DE RECONSTRUCCIÓN")
    print("=" * 40)
    
    print("Estado antes de la reconstrucción:")
    show_file_stats(sf)
    
    start_time = time.time()
    sf._rebuild()
    end_time = time.time()
    
    duration_ms = (end_time - start_time) * 1000
    
    print(f"✅ Reconstrucción completada en: {duration_ms:.2f} ms")
    
    print("Estado después de la reconstrucción:")
    show_file_stats(sf)

def stress_test(sf: SequentialIndex):
    """Prueba de estrés"""
    print("\n💪 PRUEBA DE ESTRÉS")
    print("=" * 40)
    
    num_operations = 50
    print(f"Insertando {num_operations} registros aleatorios...")
    
    start_time = time.time()
    
    inserted_ids = []
    for i in range(num_operations):
        record_id = random.randint(10000, 19999)
        name = f"StressTest_{record_id}"
        age = random.randint(18, 80)
        
        record = Record(sf.table, [record_id, name, age])
        sf.add(record)
        inserted_ids.append(record_id)
        
        if (i + 1) % 10 == 0:
            print(f"  Progreso: {i + 1}/{num_operations}")
    
    insert_time = time.time() - start_time
    print(f"✅ Inserción completada en: {insert_time*1000:.2f} ms")
    
    show_file_stats(sf)
    
    # Prueba de búsqueda masiva
    print(f"\nBuscando {len(inserted_ids)} registros...")
    start_time = time.time()
    
    found = 0
    for record_id in inserted_ids:
        result = sf.search(record_id)
        if result:
            found += 1
    
    search_time = time.time() - start_time
    print(f"✅ Búsqueda completada en: {search_time*1000:.2f} ms")
    print(f"📊 Encontrados: {found}/{len(inserted_ids)} registros")
    
    # Estadísticas de rendimiento
    print(f"\n📈 ESTADÍSTICAS DE RENDIMIENTO:")
    print(f"  Inserción promedio: {(insert_time*1000)/num_operations:.3f} ms/registro")
    print(f"  Búsqueda promedio: {(search_time*1000)/len(inserted_ids):.3f} ms/registro")

def cleanup_files(*filenames):
    """Limpia archivos de prueba"""
    for filename in filenames:
        try:
            if os.path.exists(filename):
                os.remove(filename)
                print(f"🧹 Eliminado: {filename}")
        except OSError as e:
            print(f"❌ Error eliminando {filename}: {e}")

def main():
    """Función principal de pruebas"""
    print("🚀 INICIANDO PRUEBAS DEL SEQUENTIAL FILE")
    print("=" * 60)
    
    try:
        # Configuración
        test_data_file = "test_data.dat"
        main_file = "main_file.dat"
        aux_file = "aux_file.dat"
        
        # Generar datos de prueba
        table, initial_records = generate_test_data(test_data_file, 100)
        
        # Copiar datos iniciales al archivo principal
        with open(main_file, 'wb') as f:
            for record in initial_records:
                f.write(record.pack())
        
        # Inicializar Sequential File
        sf = SequentialIndex(main_file, table)
        
        print(f"✅ Sequential File inicializado correctamente")
        show_file_stats(sf)
        display_sample_records(sf)
        
        # Ejecutar todas las pruebas
        test_insert(sf)
        test_search(sf)
        test_range_search(sf)
        test_delete(sf)
        test_rebuild(sf)
        stress_test(sf)
        
        print("\n🎉 TODAS LAS PRUEBAS COMPLETADAS EXITOSAMENTE")
        print("=" * 60)
        
        # Estadísticas finales
        show_file_stats(sf)
        display_sample_records(sf, 10)
        
        # Preguntar si limpiar archivos
        response = input("\n¿Desea eliminar los archivos de prueba? (y/n): ").lower()
        if response in ['y', 'yes', 's', 'si']:
            cleanup_files(test_data_file, main_file, aux_file)
        else:
            print("🗂️ Archivos de prueba conservados:")
            print(f"  - {main_file}")
            print(f"  - {aux_file}")
            print(f"  - {test_data_file}")
        
    except Exception as e:
        print(f"❌ Error durante las pruebas: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())