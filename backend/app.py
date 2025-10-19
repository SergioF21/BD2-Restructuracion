from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os
import csv
from pathlib import Path
import json
import traceback

# Import parser
import sys
sys.path.append(str(Path(__file__).resolve().parents[1] / 'parser'))
from sql_parser import SQLParser, ExecutionPlan

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
CORS(app)

parser = SQLParser()


def list_csv_tables():
    """Return list of csv files in data directory with basic schema (header)."""
    tables = []
    for p in DATA_DIR.glob('*.csv'):
        try:
            with p.open(newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, [])
            tables.append({'name': p.stem, 'file': str(p.resolve()), 'columns': header})
        except Exception:
            tables.append({'name': p.stem, 'file': str(p.resolve()), 'columns': []})
    return tables


def read_table(table_name, limit=None, where_fn=None):
    p = DATA_DIR / f"{table_name}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Table {table_name} not found")
    rows = []
    with p.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, r in enumerate(reader):
            if where_fn is None or where_fn(r):
                rows.append(r)
            if limit and len(rows) >= limit:
                break
    return rows


def append_row(table_name, values: dict):
    p = DATA_DIR / f"{table_name}.csv"
    exists = p.exists()
    if not exists:
        # create file with columns from values
        with p.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(values.keys()))
            writer.writeheader()
            writer.writerow(values)
        return
    with p.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or list(values.keys())
    # ensure all keys exist
    for k in values.keys():
        if k not in fieldnames:
            fieldnames.append(k)
    # read all rows and append
    with p.open('a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # if file existed but had no header, write header
        if p.stat().st_size == 0:
            writer.writeheader()
        writer.writerow(values)


def delete_rows(table_name, predicate_fn):
    p = DATA_DIR / f"{table_name}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Table {table_name} not found")
    tmp = p.with_suffix('.tmp')
    deleted = 0
    with p.open('r', newline='', encoding='utf-8') as fr, tmp.open('w', newline='', encoding='utf-8') as fw:
        reader = csv.DictReader(fr)
        fieldnames = reader.fieldnames or []
        writer = csv.DictWriter(fw, fieldnames=fieldnames)
        writer.writeheader()
        for r in reader:
            if predicate_fn(r):
                deleted += 1
                continue
            writer.writerow(r)
    tmp.replace(p)
    return deleted


@app.route('/api/tables', methods=['GET'])
def api_tables():
    try:
        return jsonify(list_csv_tables())
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/tables/search')
def api_tables_search():
    q = request.args.get('q','').lower()
    try:
        tables = list_csv_tables()
        if q:
            tables = [t for t in tables if q in t['name'].lower()]
        return jsonify(tables)
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/format', methods=['POST'])
def api_format():
    data = request.get_json() or {}
    q = data.get('query','')
    # For now, just return the trimmed query; frontend can use this as placeholder
    return jsonify({'formatted': q.strip()})


def eval_where_clause_simple(where_clause):
    """Build a simple predicate function from parser where_clause dict for basic comparisons."""
    if where_clause is None:
        return None

    t = where_clause.get('type') if isinstance(where_clause, dict) else None

    if t == 'comparison':
        field = where_clause['field']
        op = where_clause['operator']
        value = where_clause['value']

        def pred(row):
            v = row.get(field)
            if v is None:
                return False
            try:
                # compare numeric when possible
                if isinstance(value, (int, float)):
                    return eval(f"float(v){op}{repr(value)}")
                if op == '=':
                    return str(v) == str(value)
                if op == '!=':
                    return str(v) != str(value)
                if op in ['<','>','<=','>=']:
                    return eval(f"float(v){op}{float(value)}")
                return False
            except Exception:
                return False
        return pred

    if t == 'between':
        field = where_clause['field']
        a = where_clause['start']
        b = where_clause['end']
        def pred(row):
            try:
                v = float(row.get(field,0))
                return float(a) <= v <= float(b)
            except Exception:
                return False
        return pred

    if t == 'spatial':
        # expects 'field','point'(x,y),'radius'
        field = where_clause['field']
        px, py = where_clause['point']
        radius = float(where_clause['radius'])
        def pred(row):
            try:
                # assume field stores "lat,lon" or "[lat, lon]"
                raw = row.get(field)
                if raw is None:
                    return False
                if ',' in raw:
                    a,b = [float(x.strip().strip('[]')) for x in raw.split(',')[:2]]
                elif raw.startswith('['):
                    parts = raw.strip('[]').split(',')
                    a,b = float(parts[0]), float(parts[1])
                else:
                    return False
                # simple euclidean distance
                import math
                d = math.hypot(a-px, b-py)
                return d <= radius
            except Exception:
                return False
        return pred

    # compound AND/OR
    if t in ('and','or'):
        left = eval_where_clause_simple(where_clause.get('left'))
        right = eval_where_clause_simple(where_clause.get('right'))
        if t == 'and':
            def pred(row):
                return (left(row) if left else True) and (right(row) if right else True)
            return pred
        else:
            def pred(row):
                return (left(row) if left else False) or (right(row) if right else False)
            return pred

    return None


@app.route('/api/query', methods=['POST'])
def api_query():
    try:
        body = request.get_json() or {}
        q = body.get('query','')
        page = int(body.get('page',1))
        limit = int(body.get('limit',10))

        plan = parser.parse(q)

        # If parser returned ExecutionPlan object
        if isinstance(plan, ExecutionPlan):
            op = plan.operation
            data = plan.data
        elif isinstance(plan, dict):
            op = plan.get('operation') or plan.get('op') or plan.get('type')
            data = plan.get('data') or plan
        else:
            return jsonify({'error': 'No plan produced'}), 400

    # Handle CREATE TABLE FROM FILE
        if op == 'CREATE_TABLE' and data.get('source'):
            # copy the csv into data folder and register
            src = data.get('source')
            table_name = data.get('table_name') or data.get('tableName') or data.get('table')
            index_type = data.get('index_type') or data.get('indexType')
            key_field = data.get('key_field') or data.get('keyField')
            # If source is a quoted windows path, keep as-is
            src_path = Path(src)
            if not src_path.exists():
                # try removing surrounding quotes
                s2 = src.strip('"').strip("'")
                src_path = Path(s2)
            if not src_path.exists():
                return jsonify({'error': f'source file not found: {src}'}), 400
            dest = DATA_DIR / f"{table_name}.csv"
            # copy file
            import shutil
            try:
                shutil.copy(src_path, dest)
            except shutil.SameFileError:
                # source and dest are same file (already in data/) - ignore
                pass
            # store simple metadata about index in a sidecar json
            if index_type or key_field:
                meta = {'index_type': index_type, 'key_field': key_field}
                with (DATA_DIR / f"{table_name}.meta.json").open('w', encoding='utf-8') as mf:
                    json.dump(meta, mf)
            # return consistent response for frontend
            cols = []
            if dest.exists():
                with dest.open(newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cols = next(reader, [])
            return jsonify({'data': {'columns': cols, 'rows': []}, 'totalRows': 0, 'table': table_name})

    # Handle CREATE TABLE from schema (no source) - create empty csv with header
        if op == 'CREATE_TABLE' and not data.get('source'):
            table_name = data.get('table_name') or data.get('tableName') or data.get('table')
            fields = data.get('fields') or []
            cols = []
            # fields may be list of dicts with 'name'
            for f in fields:
                if isinstance(f, dict) and 'name' in f:
                    cols.append(f['name'])
                elif isinstance(f, str):
                    cols.append(f)
            if not table_name:
                return jsonify({'error': 'No table name provided'}), 400
            dest = DATA_DIR / f"{table_name}.csv"
            # create csv with header
            with dest.open('w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if cols:
                    writer.writerow(cols)
            return jsonify({'data': {'columns': cols, 'rows': []}, 'totalRows': 0, 'table': table_name})

        # SELECT
        if op == 'SELECT':
            table = data.get('table_name') or data.get('table')
            select_list = data.get('select_list', ['*'])
            where_clause = data.get('where_clause')
            pred = eval_where_clause_simple(where_clause)
            all_rows = read_table(table, limit=None, where_fn=pred)
            # apply select list
            if select_list == ['*']:
                rows = all_rows
            else:
                rows = [{k: r.get(k) for k in select_list} for r in all_rows]
            # paging
            start = (page-1)*limit
            end = start+limit
            # determine columns
            if rows:
                columns = list(rows[0].keys())
            else:
                # try to read header from csv
                p = DATA_DIR / f"{table}.csv"
                columns = []
                if p.exists():
                    with p.open(newline='', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        columns = next(reader, [])
            return jsonify({'data': {'columns': columns, 'rows': rows[start:end]}, 'totalRows': len(rows)})

        # INSERT
        if op == 'INSERT':
            table = data.get('table_name')
            values = data.get('values') or []
            # If values is a list of values not dict, try to map to columns
            if isinstance(values, list):
                # read header
                p = DATA_DIR / f"{table}.csv"
                if p.exists():
                    with p.open(newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        cols = reader.fieldnames or []
                else:
                    cols = [f'col{i+1}' for i in range(len(values))]
                row = {cols[i]: values[i] if i < len(values) else '' for i in range(len(cols))}
            elif isinstance(values, dict):
                row = values
            else:
                return jsonify({'error': 'Unsupported insert values'}), 400
            append_row(table, row)
            # return columns and empty rows
            p = DATA_DIR / f"{table}.csv"
            cols = []
            if p.exists():
                with p.open(newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cols = next(reader, [])
            return jsonify({'data': {'columns': cols, 'rows': []}, 'totalRows': 0})

        # DELETE
        if op == 'DELETE':
            table = data.get('table_name')
            where_clause = data.get('where_clause')
            pred = eval_where_clause_simple(where_clause)
            if pred is None:
                return jsonify({'error': 'DELETE without where not supported'}), 400
            deleted = delete_rows(table, pred)
            # return remaining rows count and empty rows result
            remaining = read_table(table, limit=None, where_fn=None)
            p = DATA_DIR / f"{table}.csv"
            cols = []
            if p.exists():
                with p.open(newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    cols = next(reader, [])
            return jsonify({'data': {'columns': cols, 'rows': []}, 'totalRows': len(remaining), 'deleted': deleted})

        return jsonify({'error': f'Operation not implemented: {op}', 'plan': str(plan)}), 400

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


if __name__ == '__main__':
    app.run(port=3001, debug=True)
