-- Comandos SQL de prueba para el parser
-- Ejecutar estos comandos uno por uno para probar el sistema

-- 1. Crear tabla desde archivo CSV con diferentes índices
CREATE TABLE RestaurantesBTree FROM FILE "sample_dataset.csv" USING INDEX BTree("id")

CREATE TABLE RestaurantesHash FROM FILE "sample_dataset.csv" USING INDEX ExtendibleHash("id")

CREATE TABLE RestaurantesISAM FROM FILE "sample_dataset.csv" USING INDEX ISAM("id")

CREATE TABLE RestaurantesSeq FROM FILE "sample_dataset.csv" USING INDEX SEQ("id")

-- 2. Crear tabla desde esquema definido
CREATE TABLE Restaurantes (
    id INT KEY INDEX SEQ,
    nombre VARCHAR[20] INDEX BTree,
    fechaRegistro DATE,
    ubicacion ARRAY[FLOAT] INDEX RTree
)

-- 3. Operaciones SELECT
SELECT * FROM RestaurantesBTree

SELECT * FROM RestaurantesBTree WHERE id = 5

SELECT * FROM RestaurantesBTree WHERE id BETWEEN 10 AND 20

-- 4. Operaciones INSERT
INSERT INTO Restaurantes VALUES (100, "Nuevo Restaurante", "2024-01-01", (40.4168, -3.7038))

-- 5. Operaciones DELETE
DELETE FROM RestaurantesBTree WHERE id = 15

-- 6. Búsquedas espaciales (R-tree)
SELECT * FROM Restaurantes WHERE ubicacion IN ((40.4168, -3.7038), 0.1)
