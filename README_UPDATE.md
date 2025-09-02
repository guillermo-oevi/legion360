
# Actualización OEVI (main.py + ARCA con TIPO A/B y nombre_socio)

### Archivos incluidos
- `main.py` — backend Flask actualizado (lee TIPO A/B, completa nombre_socio en Resumen ARCA, permite filtrar por `?tipo=A|B`, importa hoja Parametros si existe).
- `add_tipo_to_ventas.sql` — SQL simple para agregar la columna `tipo` a la tabla `ventas` (SQLite).
- `migrate_add_tipo.py` — script alternativo en Python que agrega la columna si no existe.

### Pasos sugeridos
1. **Detener** la app si está corriendo.
2. **Migrar DB** (una de estas opciones):
   - Opción A (SQL): `sqlite3 app.db < add_tipo_to_ventas.sql`
   - Opción B (Python): `python migrate_add_tipo.py`
3. Reemplazar tu archivo actual por `main.py` nuevo.
4. Levantar la app: `python main.py` (o `flask run`).
5. Ir a `/import/xls` y reimportar tu Excel `.xlsm/.xlsx`.
6. Verificar:
   - `/resumen-arca?ym=2025-07` muestra `tipo_comprobante` A/B y `nombre_socio`.
   - `/resumen-arca?ym=2025-07&tipo=A` filtra solo comprobantes tipo A.
   - `/totales-arca?ym=2025-07&tipo=B` agrega por tipo B.

### Notas
- La hoja **Parametros** se importa si existe. Soporta formatos:
  - columnas `Parametro`/`Valor` (genéricas), o
  - columnas sueltas: `margen_Empresa`, `margen_Vendedor` (o `margen_Vendendor`), `margen_Socio`.
- Las claves persistidas se respetan; si no existen, se crean.
- `nombre_socio` es validado contra la hoja `Socios`.
