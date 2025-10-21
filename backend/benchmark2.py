import requests
import time
import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
import random
import traceback

BASE_URL = "http://localhost:3001/api/query"
RESULTS_DIR = "benchmark2_results"

# Crear directorio para resultados
os.makedirs(RESULTS_DIR, exist_ok=True)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

def find_csv(name: str):
    p100 = DATA_DIR / (Path(name).stem + "_100" + Path(name).suffix)
    p_data = DATA_DIR / name
    p_root = BASE_DIR / name
    # prefer *_100 in data/, luego data/, luego proyecto root
    if p100.exists():
        return str(p100)
    if p_data.exists():
        return str(p_data)
    if p_root.exists():
        return str(p_root)
    return str(p_data)  # devolver ruta por defecto

class DatabaseBenchmark:
    def __init__(self):
        self.results = []
        self.failed_queries = []
        # precargar datasets para elegir IDs/puntos reales
        try:
            self.rest_df = pd.read_csv(find_csv("Restaurantes.csv"))
        except Exception:
            self.rest_df = pd.DataFrame()
        try:
            self.spatial_df = pd.read_csv(find_csv("spatial_dataset.csv"))
        except Exception:
            self.spatial_df = pd.DataFrame()

    def table_exists(self, table_name):
        """Comprueba si la tabla existe intentando un SELECT LIMIT 1."""
        q = f"SELECT * FROM {table_name} LIMIT 1"
        res = self.run_query(q, f"Check table {table_name}")
        if not res:
            return False
        return True

    def run_query(self, query, description=""):
        """Ejecutar query y retornar timing detallado"""
        try:
            start = time.perf_counter()
            response = requests.post(BASE_URL, json={"query": query}, timeout=30)
            end = time.perf_counter()

            if response.status_code != 200:
                print(f"HTTP {response.status_code} error for {description}: {response.text}")
                return None

            result = response.json()
            timing = result.get('timing', {}) if isinstance(result, dict) else {}

            return {
                'description': description,
                'query': query,
                'total_time_ms': (end - start) * 1000,
                'backend_time_ms': timing.get('total_ms', 0),
                'parse_time_ms': timing.get('parse_ms', 0),
                'exec_time_ms': timing.get('execution_ms', 0),
                'rows_returned': result.get('totalRows', 0) if isinstance(result, dict) else 0,
                'success': True
            }
        except Exception as e:
            print(f"❌ Error executing query ({description}): {e}")
            traceback.print_exc()
            return None

    def test_create_tables(self):
        """Crear tablas con diferentes índices (intenta varias sintaxis si hay fallos)."""
        print("📋 Creando tablas con diferentes índices...\n")

        csv_rest = find_csv("Restaurantes.csv")
        csv_spatial = find_csv("spatial_dataset.csv")

        # Usar nombres compatibles con la gramática/transformer:
        # el transformer espera tokens como 'SEQ','BTREE','EXTENDIBLEHASH','ISAM','RTREE'
        tables = [
            ("Restaurantes_ISAM", "ISAM"),
            ("Restaurantes_SEQ", "SEQ"),
            ("Restaurantes_BTREE", "BTREE"),
            ("Restaurantes_EXTENDIBLEHASH", "EXTENDIBLEHASH"),
        ]

        # plantillas a probar para índices con aliases
        INDEX_ALIASES = {
            "SEQ": ["SEQ", "SEQUENTIAL", "SEQUENTIAL_FILE", "SEQUENTIAL"],  # preferir 'SEQ'
            "EXTENDIBLEHASH": ["EXTENDIBLEHASH", "EXTENDIBLE_HASH", "HASH", "HASHTABLE", "HT"],
            "BTREE": ["BTREE", "BTree", "B_TREE"],
            "ISAM": ["ISAM"],
            "RTREE": ["RTREE", "R_TREE", "RTree"]
        }

        CREATE_TABLE_TEMPLATE = 'CREATE TABLE {table} FROM FILE "{csv}" USING INDEX {index_type}("id")'

        def try_create(table, index_name, csv_file=csv_rest):
            # usar template por defecto para índices simples
            q = CREATE_TABLE_TEMPLATE.format(table=table, csv=csv_file, index_type=index_name)
            return self.run_query(q, f"CREATE {index_name}")

        for table_name, index_type in tables:
            success = False
            # si es un tipo con alias, probar alternativas (prefiriendo la forma que la gramática/transformer maneja)
            candidates = INDEX_ALIASES.get(index_type, [index_type])
            for cand in candidates:
                res = try_create(table_name, cand)
                if res and res.get('success'):
                    print(f"✅ {table_name} creada con {cand} ({res['backend_time_ms']:.2f}ms)")
                    success = True
                    break
            if not success:
                print(f"❌ Error creando {table_name} (intentados: {candidates})")

        # Spatial: intentar detectar columnas numéricas en el CSV para pasar 2 campos al RTREE
        spatial_success = False
        # si tenemos el CSV cargado, buscar dos columnas numéricas que parezcan coordenadas
        numeric_cols = []
        if not self.spatial_df.empty:
            numeric_cols = list(self.spatial_df.select_dtypes(include=[np.number]).columns)

        spatial_candidates = []
        if len(numeric_cols) >= 2:
            # preferir las dos primeras numéricas (habitualmente x,y o lon,lat, o ubicacion_x,ubicacion_y)
            spatial_candidates.append((numeric_cols[0], numeric_cols[1]))
        # añadir opciones por defecto conocidas
        spatial_candidates += [("x", "y"), ("ubicacion_x", "ubicacion_y"), ("lon", "lat"), ("longitude", "latitude")]

        for f1, f2 in spatial_candidates:
            q = f'CREATE TABLE spatial_data FROM FILE "{csv_spatial}" USING INDEX RTREE({f1},{f2})'
            res = self.run_query(q, f"CREATE RTREE({f1},{f2})")
            if res and res.get('success'):
                print(f"✅ spatial_data creada con campos ({f1},{f2}).")
                spatial_success = True
                break

        if not spatial_success:
            print("❌ Error creando spatial_data. Revisa la sintaxis esperada por el parser y los nombres de columnas del CSV.")
        print()

    def _sample_rest_ids(self, n=5):
        if self.rest_df.empty:
            return [1, 5, 10, 25, 50][:n]
        ids = list(self.rest_df['id'].dropna().astype(int).unique())
        ids.sort()
        # elegir una mezcla: pequeño, medio, grande
        picks = []
        picks.append(ids[0])
        picks.append(ids[min(2, len(ids)-1)])
        picks.append(ids[min(len(ids)//2, len(ids)-1)])
        picks.append(ids[-1])
        # completar aleatoriamente si falta
        while len(picks) < n:
            picks.append(random.choice(ids))
        return list(dict.fromkeys(picks))[:n]

    def _sample_spatial_points(self, n=5):
        if self.spatial_df.empty:
            return [(50.0, 50.0) for _ in range(n)]
        pts = list(self.spatial_df[['x', 'y']].itertuples(index=False, name=None))
        # sample a few existing points and also slightly jittered points
        chosen = random.sample(pts, min(n, len(pts)))
        while len(chosen) < n:
            x, y = random.choice(pts)
            chosen.append((x + random.uniform(-0.01, 0.01), y + random.uniform(-0.01, 0.01)))
        return chosen[:n]

    def test_point_queries(self, iterations=10):
        """Prueba 1: Búsqueda por punto (WHERE id = x) incluyendo R_TREE point queries"""
        print("🔍 Prueba 1: Búsqueda por Punto\n")

        test_ids = self._sample_rest_ids(n=5)
        spatial_points = self._sample_spatial_points(n=5)

        # punto para tablas tradicionales
        for test_id in test_ids:
            for index_type in ["ISAM", "SEQ", "BTREE", "EXTENDIBLEHASH"]:
                table_name = f"Restaurantes_{index_type}"
                if not self.table_exists(table_name):
                    print(f"⚠️ {table_name} no existe — se omite prueba punto.")
                    continue

                times = []
                for _ in range(iterations):
                    query = f"SELECT * FROM {table_name} WHERE id = {test_id}"
                    result = self.run_query(query, f"Point Query - {index_type}")
                    if result:
                        times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
                    else:
                        self.failed_queries.append(query)
                if times:
                    avg = np.mean(times)
                    self.results.append({
                        'test': 'point_query',
                        'index': index_type,
                        'parameter': test_id,
                        'avg_time_ms': float(avg),
                        'std_time_ms': float(np.std(times)),
                        'min_time_ms': float(min(times)),
                        'max_time_ms': float(max(times))
                    })
                    print(f"{index_type:10} ID={test_id:5} → {avg:6.2f}ms")

        # puntos para spatial (igualdad exacta sobre x/y puede no devolver, usamos coordenadas existentes)
        if not self.table_exists("spatial_data"):
            print("⚠️ spatial_data no existe — se omiten pruebas puntuales espaciales.")
        else:
            for (x, y) in spatial_points:
                times = []
                for _ in range(iterations):
                    query = f"SELECT * FROM spatial_data WHERE x = {x} AND y = {y}"
                    result = self.run_query(query, "Point Query - R_TREE")
                    if result:
                        times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
                    else:
                        self.failed_queries.append(query)
                if times:
                    avg = np.mean(times)
                    self.results.append({
                        'test': 'point_query',
                        'index': 'R_TREE',
                        'parameter': f"{x:.4f},{y:.4f}",
                        'avg_time_ms': float(avg),
                        'std_time_ms': float(np.std(times)),
                        'min_time_ms': float(min(times)),
                        'max_time_ms': float(max(times))
                    })
                    print(f"R_TREE    PT=({x:.4f},{y:.4f}) → {avg:6.2f}ms")

        print()

    def test_range_queries(self, iterations=10):
        """Prueba 2: Búsqueda por rango (WHERE id BETWEEN x AND y)"""
        print("📊 Prueba 2: Búsqueda por Rango\n")

        # si hay pocos ids, crear rangos pequeños
        ids = sorted(self.rest_df['id'].dropna().astype(int).unique()) if not self.rest_df.empty else list(range(1, 101))
        max_id = ids[-1] if ids else 100
        ranges = [
            (1, min(5, max_id)),
            (1, min(10, max_id)),
            (1, min(25, max_id)),
            (1, min(50, max_id)),
            (1, max_id)
        ]

        for start, end in ranges:
            range_size = end - start + 1
            for index_type in ["ISAM", "SEQ", "BTREE"]:
                times = []
                for _ in range(iterations):
                    query = f"SELECT * FROM Restaurantes_{index_type} WHERE id BETWEEN {start} AND {end}"
                    result = self.run_query(query, f"Range Query - {index_type}")
                    if result:
                        times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
                if times:
                    avg = np.mean(times)
                    self.results.append({
                        'test': 'range_query',
                        'index': index_type,
                        'parameter': range_size,
                        'avg_time_ms': float(avg),
                        'std_time_ms': float(np.std(times)),
                        'min_time_ms': float(min(times)),
                        'max_time_ms': float(max(times))
                    })
                    print(f"{index_type:10} Range size={range_size:5} → {avg:6.2f}ms")

        print()

    def test_insert_performance(self, iterations=5):
        """Prueba 3: Rendimiento de INSERT"""
        print("➕ Prueba 3: Rendimiento de INSERT\n")

        # Restaurantes.csv columns: id,nombre,fechaRegistro,ubicacion_x,ubicacion_y,rating
        for index_type in ["ISAM", "SEQ", "BTREE", "EXTENDIBLEHASH"]:
            table_name = f"Restaurantes_{index_type}"
            if not self.table_exists(table_name):
                print(f"⚠️ {table_name} no existe — se omite prueba de INSERT para este índice.")
                continue

            times = []
            for i in range(iterations):
                new_id = int(time.time() * 1000) % 1000000 + i
                query = f"INSERT INTO {table_name} VALUES ({new_id}, 'Benchmark Test {i}', '2025-01-01', -12.05, -77.04, 4.0)"
                result = self.run_query(query, f"Insert - {index_type}")
                if result:
                    times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
                else:
                    self.failed_queries.append(query)
            if times:
                avg = np.mean(times)
                self.results.append({
                    'test': 'insert',
                    'index': index_type,
                    'parameter': iterations,
                    'avg_time_ms': float(avg),
                    'std_time_ms': float(np.std(times)),
                    'min_time_ms': float(min(times)),
                    'max_time_ms': float(max(times))
                })
                print(f"{index_type:10} → {avg:6.2f}ms")

        # INSERT en spatial_data: usar id explícito (no DEFAULT) y chequear existencia de tabla
        if not self.spatial_df.empty:
            xs = self.spatial_df['x']
            ys = self.spatial_df['y']
            cx, cy = xs.mean(), ys.mean()
        else:
            cx, cy = 50.0, 50.0

        times = []
        for i in range(iterations):
            # usar id explícito
            new_id = int(time.time() * 1000) % 1000000 + i + 1000000
            x = cx + random.uniform(-0.01, 0.01)
            y = cy + random.uniform(-0.01, 0.01)
            query = f'INSERT INTO spatial_data VALUES ({new_id}, {x}, {y})'
            result = self.run_query(query, "Insert - R_TREE")
            if result:
                times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
        if times:
            avg = np.mean(times)
            self.results.append({
                'test': 'insert',
                'index': 'R_TREE',
                'parameter': iterations,
                'avg_time_ms': float(avg),
                'std_time_ms': float(np.std(times)),
                'min_time_ms': float(min(times)),
                'max_time_ms': float(max(times))
            })
            print(f"R_TREE    → {avg:6.2f}ms")

        print()

    def test_range_queries_bbox(self, iterations=10):
        """Prueba 4: Búsqueda por Rango en Datos Espaciales (bbox)"""
        print("📊 Prueba 4: Búsqueda por Rango en Datos Espaciales\n")

        if self.spatial_df.empty:
            print("⚠️ spatial_dataset.csv no encontrado o vacío; omitiendo pruebas espaciales bbox.")
            return

        xs = self.spatial_df['x']
        ys = self.spatial_df['y']
        xmin, xmax = xs.min(), xs.max()
        ymin, ymax = ys.min(), ys.max()

        ranges = [
            (xmin, xmin + (xmax-xmin)*0.1, ymin, ymin + (ymax-ymin)*0.1),
            (xmin, xmin + (xmax-xmin)*0.2, ymin, ymin + (ymax-ymin)*0.2),
            (xmin, xmin + (xmax-xmin)*0.5, ymin, ymin + (ymax-ymin)*0.5),
            (xmin, xmax, ymin, ymax)
        ]

        for x_start, x_end, y_start, y_end in ranges:
            times = []
            for _ in range(iterations):
                query = f"SELECT * FROM spatial_data WHERE x BETWEEN {x_start} AND {x_end} AND y BETWEEN {y_start} AND {y_end}"
                result = self.run_query(query, "Spatial Range Query")
                if result:
                    times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
            if times:
                avg = np.mean(times)
                self.results.append({
                    'test': 'spatial_range_query',
                    'index': 'R_TREE',
                    'parameter': f"{x_end-x_start:.6f}x{y_end-y_start:.6f}",
                    'avg_time_ms': float(avg),
                    'std_time_ms': float(np.std(times)),
                    'min_time_ms': float(min(times)),
                    'max_time_ms': float(max(times))
                })
                print(f"RANGE bbox {x_end-x_start:.4f}x{y_end-y_start:.4f} → {avg:6.2f}ms")

        print()

    def test_range_queries_radios(self, iterations=10):
        """Prueba 5: Búsqueda por Rango Circular en Datos Espaciales (usa expresión algebraica en vez de SQRT)"""
        print("📊 Prueba 5: Búsqueda por Rango Circular en Datos Espaciales\n")

        if self.spatial_df.empty or not self.table_exists("spatial_data"):
            print("⚠️ spatial_dataset.csv no encontrado/tabla no creada; omitiendo pruebas espaciales circulares.")
            return

        # elegir centro como promedio del dataset
        cx, cy = self.spatial_df['x'].mean(), self.spatial_df['y'].mean()
        radios = [0.1, 0.5, 1.0, 5.0]  # unidades compatibles con los datos

        for radio in radios:
            r2 = radio * radio
            times = []
            for _ in range(iterations):
                # usar comparación algebraica para evitar funciones SQL no soportadas
                query = f"SELECT * FROM spatial_data WHERE (x - {cx})*(x - {cx}) + (y - {cy})*(y - {cy}) <= {r2}"
                result = self.run_query(query, "Spatial Circular Range Query")
                if result:
                    times.append(result.get('exec_time_ms', result.get('backend_time_ms', 0)))
                else:
                    self.failed_queries.append(query)
            if times:
                avg = np.mean(times)
                self.results.append({
                    'test': 'spatial_circular_range_query',
                    'index': 'R_TREE',
                    'parameter': radio,
                    'avg_time_ms': float(avg),
                    'std_time_ms': float(np.std(times)),
                    'min_time_ms': float(min(times)),
                    'max_time_ms': float(max(times))
                })
                print(f"RADIO={radio:6} → {avg:6.2f}ms")

        print()

    def generate_graphs(self):
        """Generar todas las gráficas"""
        if not self.results:
            print("⚠️ No hay resultados para generar gráficas.")
            return
        df = pd.DataFrame(self.results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # gráficas existentes
        self._plot_point_queries(df, timestamp)
        self._plot_range_queries(df, timestamp)
        self._plot_insert_performance(df, timestamp)
        # gráficas espaciales
        self._plot_spatial_queries(df, timestamp)

        self._plot_summary_table(df, timestamp)
        self._save_results(df, timestamp)

    # (las funciones de plot point/range/insert/summary se mantienen muy parecidas a las tuyas,
    #  añadimos una para spatial)
    def _plot_point_queries(self, df, timestamp):
        point_data = df[df['test'] == 'point_query']
        if point_data.empty:
            return
        fig, ax = plt.subplots(figsize=(10, 6))
        for index in point_data['index'].unique():
            data = point_data[point_data['index'] == index]
            # para spatial, parameter es string "x,y"; no ordenar numéricamente
            if index == 'R_TREE':
                xvals = list(range(len(data)))
            else:
                xvals = data['parameter'].astype(float)
            ax.plot(xvals, data['avg_time_ms'], marker='o', label=index)
        ax.set_ylabel('Tiempo Promedio (ms)')
        ax.set_title('Búsqueda por Punto')
        ax.legend()
        plt.tight_layout()
        out = f'{RESULTS_DIR}/point_queries_{timestamp}.png'
        plt.savefig(out, dpi=200)
        print(f"✅ Gráfica guardada: {Path(out).name}")
        plt.close()

    def _plot_range_queries(self, df, timestamp):
        range_data = df[df['test'] == 'range_query']
        if range_data.empty:
            return
        fig, ax = plt.subplots(figsize=(10, 6))
        for index in range_data['index'].unique():
            data = range_data[range_data['index'] == index].sort_values('parameter')
            ax.plot(data['parameter'], data['avg_time_ms'], marker='s', label=index)
        ax.set_xlabel('Tamaño del rango')
        ax.set_ylabel('Tiempo Promedio (ms)')
        ax.set_title('Búsqueda por Rango')
        ax.legend()
        plt.tight_layout()
        out = f'{RESULTS_DIR}/range_queries_{timestamp}.png'
        plt.savefig(out, dpi=200)
        print(f"✅ Gráfica guardada: {Path(out).name}")
        plt.close()

    def _plot_insert_performance(self, df, timestamp):
        insert_data = df[df['test'] == 'insert']
        if insert_data.empty:
            return
        fig, ax = plt.subplots(figsize=(8, 5))
        grouped = insert_data.groupby('index')['avg_time_ms'].mean()
        bars = ax.bar(grouped.index, grouped.values)
        ax.set_ylabel('Tiempo Promedio (ms)')
        ax.set_title('INSERT performance')
        plt.tight_layout()
        out = f'{RESULTS_DIR}/insert_performance_{timestamp}.png'
        plt.savefig(out, dpi=200)
        print(f"✅ Gráfica guardada: {Path(out).name}")
        plt.close()

    def _plot_spatial_queries(self, df, timestamp):
        sp_range = df[df['test'] == 'spatial_range_query']
        sp_circ = df[df['test'] == 'spatial_circular_range_query']
        if not sp_range.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            params = list(range(len(sp_range)))
            ax.plot(params, sp_range['avg_time_ms'], marker='o', label='bbox')
            ax.set_title('Spatial bbox queries')
            ax.set_ylabel('Tiempo Promedio (ms)')
            plt.tight_layout()
            out = f'{RESULTS_DIR}/spatial_bbox_{timestamp}.png'
            plt.savefig(out, dpi=200)
            print(f"✅ Gráfica guardada: {Path(out).name}")
            plt.close()
        if not sp_circ.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.plot(sp_circ['parameter'], sp_circ['avg_time_ms'], marker='o', label='circle')
            ax.set_title('Spatial circular queries')
            ax.set_ylabel('Tiempo Promedio (ms)')
            plt.tight_layout()
            out = f'{RESULTS_DIR}/spatial_circular_{timestamp}.png'
            plt.savefig(out, dpi=200)
            print(f"✅ Gráfica guardada: {Path(out).name}")
            plt.close()

    def _plot_summary_table(self, df, timestamp):
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.axis('tight'); ax.axis('off')
        summary_data = []
        for index in df['index'].unique():
            index_data = df[df['index'] == index]
            point_avg = index_data[index_data['test'] == 'point_query']['avg_time_ms'].mean()
            range_avg = index_data[index_data['test'] == 'range_query']['avg_time_ms'].mean()
            insert_avg = index_data[index_data['test'] == 'insert']['avg_time_ms'].mean()
            summary_data.append([index,
                                 f"{point_avg:.2f}" if not np.isnan(point_avg) else "N/A",
                                 f"{range_avg:.2f}" if not np.isnan(range_avg) else "N/A",
                                 f"{insert_avg:.2f}" if not np.isnan(insert_avg) else "N/A"])
        table = ax.table(cellText=summary_data, colLabels=['Índice','Punto','Rango','INSERT'], loc='center')
        table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 2)
        out = f'{RESULTS_DIR}/summary_table_{timestamp}.png'
        plt.savefig(out, dpi=200, bbox_inches='tight')
        print(f"✅ Tabla guardada: {Path(out).name}")
        plt.close()

    def _save_results(self, df, timestamp):
        json_file = f'{RESULTS_DIR}/results_{timestamp}.json'
        df.to_json(json_file, orient='records', indent=2)
        csv_file = f'{RESULTS_DIR}/results_{timestamp}.csv'
        df.to_csv(csv_file, index=False)
        # guardar queries fallidas para debug
        failed_file = Path(RESULTS_DIR) / f'results_failed_queries_{timestamp}.txt'
        if self.failed_queries:
            with open(failed_file, 'w', encoding='utf-8') as f:
                for q in self.failed_queries:
                    f.write(q.replace('\n', ' ') + '\n')
            print(f"⚠️ Queries fallidas guardadas: {failed_file.name}")
        print(f"✅ Resultados JSON/CSV guardados: results_{timestamp}.json, results_{timestamp}.csv")


def main():
    print("=" * 60)
    print("  SISTEMA DE BENCHMARKING - BASE DE DATOS")
    print("=" * 60)
    print()

    benchmark = DatabaseBenchmark()

    # Ejecutar pruebas
    benchmark.test_create_tables()
    benchmark.test_point_queries(iterations=10)
    benchmark.test_range_queries(iterations=10)
    benchmark.test_insert_performance(iterations=5)
    benchmark.test_range_queries_bbox(iterations=10)
    benchmark.test_range_queries_radios(iterations=10)

    # Generar gráficas
    print("\n📊 Generando gráficas...")
    benchmark.generate_graphs()

    print("\n✅ Benchmark completado!")
    print(f"📁 Resultados guardados en: {RESULTS_DIR}/")


if __name__ == '__main__':
    main()