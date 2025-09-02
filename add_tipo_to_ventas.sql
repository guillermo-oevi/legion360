
-- add_tipo_to_ventas.sql
-- Ejecutar una sola vez contra SQLite app.db
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;

-- Verificar si la columna ya existe (nota: SQLite no soporta IF NOT EXISTS en ALTER COLUMN);
-- Este script asume que no existe. Si ya existe, simplemente no lo ejecutes.
ALTER TABLE ventas ADD COLUMN tipo TEXT;

COMMIT;
PRAGMA foreign_keys=on;
