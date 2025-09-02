-- Parches sugeridos para garantizar consistencia y performance en ARCA
-- Ajustá nombres de tablas/columnas si difieren en tu proyecto.

-- 1) Normalización de tipos a mayúsculas (opcional)
-- UPDATE compras SET tipo = UPPER(TRIM(tipo));
-- UPDATE ventas  SET tipo = UPPER(TRIM(tipo));

-- 2) Constraints para permitir solo valores válidos
-- Postgres:
ALTER TABLE compras ADD CONSTRAINT IF NOT EXISTS chk_tipo_comprobante_compra
  CHECK (tipo IN ('A','B','N'));
ALTER TABLE ventas  ADD CONSTRAINT IF NOT EXISTS chk_tipo_comprobante_venta
  CHECK (tipo IN ('A','B','N'));

-- 3) Índices por (tipo, fecha) aceleran Resumen/Totales ARCA
CREATE INDEX IF NOT EXISTS idx_compras_tipo_fecha ON compras (tipo, fecha);
CREATE INDEX IF NOT EXISTS idx_ventas_tipo_fecha  ON ventas  (tipo, fecha);

-- SQLite no soporta CHECK de la misma forma ni IF NOT EXISTS en constraints;
-- de ser necesario, creá tablas con CHECK o validá en aplicación.
