from core.models import Table, Field, Record
from core.databasemanager import DatabaseManager
import os

DATAFILE = "personas_isam.dat"

def clean_files():
    for suffix in [DATAFILE, DATAFILE.replace('.dat', '.idx'), DATAFILE.replace('.dat', '.header')]:
        if os.path.exists(suffix):
            os.remove(suffix)

def run_once():
    # Definir tabla (misma estructura que example_persistence)
    fields = [
        Field("id", int),
        Field("nombre", str, 20),
        Field("edad", int),
        Field("ciudad", str, 15)
    ]
    table = Table("personas", fields, "id")

    # Crear DatabaseManager usando ISAM como índice
    db = DatabaseManager(table, DATAFILE, order=3, index_type='isam')

    print("Índice cargado? ", not db.index.is_empty())

    # Agregar registros (algunos duplicados para probar overflow)
    registros = [
        [1, "Juan Pérez", 25, "Madrid"],
        [2, "María García", 30, "Barcelona"],
        [3, "Carlos López", 22, "Valencia"],
        [2, "María García (dup)", 31, "Barcelona"],  # clave 2 duplicada -> overflow
        [4, "Ana Martín", 28, "Sevilla"],
    ]

    for vals in registros:
        rec = Record(table, vals)
        db.add_record(rec)

    print("\n--- Después de insertar ---")

    # Buscar clave con overflow (usamos la API isam si existe)
    if hasattr(db.index, 'get_all_positions'):
        print("\nPosiciones para clave 2:", db.index.get_all_positions(2))
    else:
        print("\nPosiciones (search simple) para clave 2 base pos:", db.index.search(2))

    # Range search
    print("\nRange 1..3:")
    for r in db.range_search(1, 3):
        print(" ->", r.values)

    # Guardar
    db.save_all()
    print("\nArchivos generados:")
    print(" - data:", db.data_filename)
    print(" - header:", db.file_manager.header_filename)
    print(" - index:", db.index_filename)

if __name__ == "__main__":
    # Empezar limpio para la primera corrida
    clean_files()
    print("CORRIDA 1: crear e insertar")
    run_once()

    # Segunda corrida: cargar desde disco y verificar persistencia
    print("\n\nCORRIDA 2: recargar y verificar index cargado desde archivo")
    # No limpiamos archivos ahora; el DatabaseManager debería cargar el índice
    from time import sleep
    sleep(1)
    # volver a crear manager que ahora debe cargar el .idx
    fields = [
        Field("id", int),
        Field("nombre", str, 20),
        Field("edad", int),
        Field("ciudad", str, 15)
    ]
    table = Table("personas", fields, "id")
    db2 = DatabaseManager(table, DATAFILE, order=3, index_type='isam')
    print("Índice vacío?:", db2.index.is_empty())
    if hasattr(db2.index, 'get_all_positions'):
        print("Posiciones para clave 2 (desde recarga):", db2.index.get_all_positions(2))
    else:
        print("Pos base para clave 2 (desde recarga):", db2.index.search(2))
