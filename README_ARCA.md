# OEVI – ARCA (solo comprobantes A/B)

Este paquete aplica el filtro **A/B** en todas las consultas de **Resumen ARCA** y
**Totales ARCA**, e incluye herramientas para validar contra tu Excel modelo.

## Archivos incluidos

- `app/services/arca.py` → Servicio central con:
  - `get_resumen_arca(desde,hasta)`: une compras+ventas filtrando `tipo in ('A','B')`.
  - `get_totales_arca(desde,hasta)`: totales por mes y tipo de operación.
  - `compute_totales_arca(rows)`: agrupación y cálculo de Saldo Técnico IVA.
- `app/blueprints/arca/routes.py` → Endpoints JSON:
  - `GET /arca/resumen?desde=YYYY-MM-DD&hasta=YYYY-MM-DD`
  - `GET /arca/totales?desde=YYYY-MM-DD&hasta=YYYY-MM-DD`
- `db/patches/2025-08-24-idx-and-checks.sql` → Índices y checks sugeridos.
- `scripts/validar_arca_desde_excel.py` → Validador desde Excel (exporta CSVs).
- `tests/test_arca_filter.py` → Tests base de la agregación y constantes.

## Cómo integrarlo

1. **Copiá** las carpetas `app/services`, `app/blueprints/arca`, `db/patches`, `scripts`, `tests`
   dentro de tu repo, respetando la estructura (o ajustá imports si usás otra).

2. **Registrá el blueprint** en tu *app factory* (por ejemplo `app/__init__.py`):

   ```python
   from app.blueprints.arca.routes import bp as arca_bp
   app.register_blueprint(arca_bp)
   ```

3. **Ejecutá el parche SQL** (Postgres):

   ```bash
   psql "$DATABASE_URL" -f db/patches/2025-08-24-idx-and-checks.sql
   ```

   > Ajustá los nombres de tablas/columnas si difieren (`compras`, `ventas`, `tipo`, `fecha`).

4. **Probar endpoints**:

   - Resumen: `GET /arca/resumen?desde=2025-07-01&hasta=2025-07-31`
   - Totales: `GET /arca/totales?desde=2025-07-01&hasta=2025-07-31`

   Ambos deben **excluir** comprobantes cuyo `tipo` no sea `A` o `B` (p.ej. `N`).

5. **Validar con tu Excel** (opcional):

   ```bash
   python scripts/validar_arca_desde_excel.py "OEVI_modelo_v3 6.xlsm" 2025-07
   ```

   Esto genera `Resumen_ARCA_filtrado.csv` y `Totales_ARCA_filtrado.csv` para comparar con tu web.

## Notas
- Si tu proyecto usa SQLite, los índices funcionan; los **CHECK** podrían requerir cambios.
- Si tus modelos no se llaman `Compra`/`Venta` o tus columnas tienen otro nombre,
  ajustá las importaciones y alias en `app/services/arca.py`.
- Para performance, mantené índices en `(tipo, fecha)`.

---

**Fecha del patch:** 2025-08-24
