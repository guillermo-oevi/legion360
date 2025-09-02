
# OEVI — Importar desde Google Sheets (público) y Excel

## Requisitos
```
pip install -U flask flask_sqlalchemy pandas openpyxl requests
```

## Archivos
- `main_full_fixed.py`: app completa (Dashboard, Resumen ARCA, Totales ARCA, Resumen Socio, Import XLS/GSheet, auto‑migración SQLite)
- `templates/import_xls.html`: pantalla con botón/campo para importar desde Google Sheets (por ID o URL) y subir Excel
- `templates/base_nav_patch.html` (opcional): base de referencia con navbar apuntando a los endpoints esperados
- `requirements.txt`: dependencias

## Pasos rápidos
1. Hacé backup de `app.db` (opcional):
   ```bash
   cp app.db backups/app_backup_$(date +%F_%H%M).db
   ```
2. Copiá `main_full_fixed.py` como `main.py` en tu proyecto.
3. Colocá `templates/import_xls.html` en la carpeta `templates/` (no pisa tus otras vistas).
4. (Opcional) Si querés una base de referencia de navbar, usá `templates/base_nav_patch.html` como guía para ajustar tu `base.html`.
5. Ejecutá:
   ```bash
   python main.py
   ```
6. Abrí `/import/xls` y probá el botón **Importar desde Sheet** (trae el ID por defecto preconfigurado en `main.py`).

## Notas
- El Sheet debe estar compartido como **“Cualquiera con el enlace (lector)”**.
- Hojas esperadas: `Parametros`, `Socios`, `FactCompras`, `FactVentas`.
- `FactCompras` soporta `personal` y `iva_deducible_pct`. Si `iva_deducible_pct` está vacío, se usa el default según el tipo (normal/personal) definido en `Parametros`.
