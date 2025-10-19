# Sistema SQL con Múltiples Estructuras de Datos

## 📋 Descripción

Sistema completo de parser y executor SQL que soporta múltiples estructuras de datos:
- **B+ Tree** - Árbol B+ para índices ordenados
- **Extendible Hashing** - Hashing extensible para acceso rápido
- **ISAM** - Índice Secuencial de Acceso Múltiple
- **Sequential File** - Archivo secuencial con índice
- **R-tree** - Árbol R para datos espaciales

## 🏗️ Arquitectura

### Separación de Responsabilidades
- **Parser** (`sql_parser.py`) - Convierte SQL → ExecutionPlan (sin side-effects)
- **Executor** (`sql_executor.py`) - Ejecuta ExecutionPlan sobre estructuras de datos
- **REPL** (`sql_repl.py`) - Interfaz interactiva con logging y manejo de errores

### Archivos Principales
```
parser/
├── grammar.py           # Gramática EBNF mejorada
├── sql_parser.py        # Parser que devuelve ExecutionPlan
├── sql_executor.py      # Executor que ejecuta planes
├── sql_repl.py          # REPL interactivo
├── test_parser_unit.py  # Tests unitarios del parser
├── test_end_to_end.py   # Tests end-to-end
└── run_all_tests.py     # Ejecuta todos los tests
```

## 🚀 Uso

### 1. REPL Interactivo
```bash
cd parser
python sql_repl.py
```

### 2. Ejecutar Archivo SQL
```bash
cd parser
python sql_repl.py -f archivo.sql
```

### 3. Modo Verbose
```bash
cd parser
python sql_repl.py -v
```

### 4. Ejecutar Tests
```bash
cd parser
python run_all_tests.py
```

## 📝 Comandos SQL Soportados

### CREATE TABLE
```sql
-- Desde esquema
CREATE TABLE Restaurantes (
    id INT KEY INDEX SEQ,
    nombre VARCHAR[20] INDEX BTree,
    precio FLOAT,
    ubicacion ARRAY[FLOAT] INDEX RTree
);

-- Desde archivo CSV
CREATE TABLE Restaurantes FROM FILE "datos.csv" USING INDEX BTree("id");
```

### SELECT
```sql
-- Todos los registros
SELECT * FROM Restaurantes;

-- Con condición WHERE
SELECT * FROM Restaurantes WHERE id = 5;

-- Con BETWEEN
SELECT * FROM Restaurantes WHERE precio BETWEEN 20 AND 50;

-- Búsqueda espacial (R-tree)
SELECT * FROM Restaurantes WHERE ubicacion IN ((40.4168, -3.7038), 0.1);
```

### INSERT
```sql
INSERT INTO Restaurantes VALUES (100, "Nuevo Restaurante", 25.50);
```

### DELETE
```sql
DELETE FROM Restaurantes WHERE id = 100;
```

### Comandos Especiales (REPL)
- `.help` - Mostrar ayuda
- `.tables` - Listar tablas creadas
- `.info tabla` - Información de tabla
- `.verbose` - Activar/desactivar modo verbose
- `.exit` - Salir

## 🔧 Tipos de Índices

| Índice | Descripción | Mejor para |
|--------|-------------|------------|
| `SEQ` | Archivo secuencial | Datos ordenados, acceso secuencial |
| `BTree` | Árbol B+ | Búsquedas por rango, datos ordenados |
| `ExtendibleHash` | Hashing extensible | Acceso directo por clave |
| `ISAM` | Índice secuencial | Datos semi-estáticos, búsquedas por rango |
| `RTree` | Árbol R | Datos espaciales, búsquedas por proximidad |

## 🧪 Testing

### Tests Unitarios (Parser)
```bash
cd parser
python test_parser_unit.py
```

### Tests End-to-End
```bash
cd parser
python test_end_to_end.py
```

### Todos los Tests
```bash
cd parser
python run_all_tests.py
```

## 📊 Ejemplo Completo

```sql
-- 1. Crear tabla desde CSV
CREATE TABLE Productos FROM FILE "sample_dataset.csv" USING INDEX BTree("id");

-- 2. Consultar todos los productos
SELECT * FROM Productos;

-- 3. Buscar producto específico
SELECT * FROM Productos WHERE id = 5;

-- 4. Buscar por rango de precios
SELECT * FROM Productos WHERE precio BETWEEN 20 AND 50;

-- 5. Insertar nuevo producto
INSERT INTO Productos VALUES (100, "Nuevo Producto", 35.99, "Categoria");

-- 6. Eliminar producto
DELETE FROM Productos WHERE id = 100;
```

## 🔍 Características Avanzadas

### Gramática Robusta
- Soporte para comillas simples y dobles
- Tokens robustos (ESCAPED_STRING, SIGNED_NUMBER)
- Manejo de comentarios (-- y /* */)
- Múltiples statements por archivo

### Manejo de Errores
- Errores de sintaxis con posición
- Errores de ejecución descriptivos
- Logging detallado en modo verbose
- Validación de tipos de datos

### Persistencia
- Todas las estructuras soportan persistencia
- Archivos de índice automáticos
- Carga automática al reiniciar

## 🛠️ Desarrollo

### Agregar Nuevo Tipo de Índice
1. Implementar en `sql_executor.py` método `_create_structure()`
2. Agregar a la gramática en `grammar.py`
3. Actualizar tests en `test_end_to_end.py`

### Agregar Nueva Operación SQL
1. Extender gramática en `grammar.py`
2. Actualizar transformer en `sql_parser.py`
3. Implementar ejecución en `sql_executor.py`
4. Agregar tests

## 📈 Rendimiento

- **Parser**: O(n) donde n es la longitud del comando
- **B+ Tree**: O(log n) para búsquedas
- **Extendible Hashing**: O(1) promedio para búsquedas
- **ISAM**: O(log n) para búsquedas
- **R-tree**: O(log n) para búsquedas espaciales

## 🐛 Troubleshooting

### Error: "No module named 'lark'"
```bash
pip install lark-parser
```

### Error: "Archivo no encontrado"
- Verificar que el archivo CSV existe
- Usar rutas absolutas si es necesario

### Error: "Tabla no existe"
- Verificar que la tabla fue creada correctamente
- Usar `.tables` para listar tablas disponibles

