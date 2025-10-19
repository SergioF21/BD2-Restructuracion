from rtree import RTreeIndex
import csv

# Asumimos que RTreeIndex ya está definido (como en tu código)
class Field:
    def __init__(self, name):
        self.name = name

fields = [Field("x"), Field("y")]
rt_index = RTreeIndex("rtree.idx", fields)

# Cargar desde el CSV
with open("spatial_dataset.csv", newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        record = {"id": int(row["id"]), "x": float(row["x"]), "y": float(row["y"])}
        rt_index.insert(record)

# Ejemplo: búsqueda de intersección (bbox)
bbox = (20, 20, 40, 40)
result = rt_index.range_search(bbox)
print("IDs dentro del bbox:", result)

# Ejemplo: búsqueda radial
point = (30, 30)
radius = 10
result = rt_index.rtree.range_search_radio(point, radius)
print("IDs dentro del radio:", result)

# Ejemplo: K vecinos más cercanos
k = 3
result = rt_index.rtree.range_search_k(point, k)
print(f"{k} vecinos más cercanos:", result)
