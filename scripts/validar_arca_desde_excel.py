# -*- coding: utf-8 -*-
"""
Valida la lógica de ARCA directamente desde un Excel de modelo como
"OEVI_modelo_v3 6.xlsm". Filtra solo comprobantes TIPO A/B y exporta:
 - Resumen_ARCA_filtrado.csv
 - Totales_ARCA_filtrado.csv

Uso:
  python scripts/validar_arca_desde_excel.py ruta/al/archivo.xlsm [YYYY-MM]

Requiere: pandas, openpyxl
"""
import sys
import pandas as pd
from pathlib import Path

ALLOWED = {"A","B"}


def _read_sheet(xls, name):
    try:
        return pd.read_excel(xls, sheet_name=name)
    except Exception:
        return None


def _std_cols(df, tipo_op):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    # Tipos
    if 'TIPO' in df.columns:
        df['tipo_comprobante'] = df['TIPO'].astype(str).str.strip().str.upper()
    elif 'tipo' in df.columns:
        df['tipo_comprobante'] = df['tipo'].astype(str).str.strip().str.upper()
    else:
        df['tipo_comprobante'] = None

    # Denominación
    if tipo_op == 'COMPRA':
        den = 'PROVEEDOR' if 'PROVEEDOR' in df.columns else 'proveedor'
    else:
        den = 'CLIENTE' if 'CLIENTE' in df.columns else 'cliente'
    if den not in df.columns:
        df[den] = None

    # CUIT
    cuit_col = 'CUIT' if 'CUIT' in df.columns else ('CUIT_VENTA' if 'CUIT_VENTA' in df.columns else None)
    if cuit_col is None:
        df['CUIT'] = None
    elif cuit_col != 'CUIT':
        df['CUIT'] = df[cuit_col]

    # Origen/Destino
    od = 'ORIGEN' if 'ORIGEN' in df.columns else ('DESTINO' if 'DESTINO' in df.columns else None)
    if od is None:
        df['ORIGEN_DESTINO'] = None
    else:
        df['ORIGEN_DESTINO'] = df[od]

    # Montos
    for col in ['PESOS_SIN_IVA','IVA_21','IVA_105','TOTAL_CON_IVA']:
        if col not in df.columns:
            df[col] = 0

    # Fecha / Nro factura
    for col in ['FECHA','NRO_FACTURA']:
        if col not in df.columns:
            df[col] = None

    # Socio
    if 'nombre_socio' not in df.columns:
        df['nombre_socio'] = None

    # Filtro A/B
    df = df[df['tipo_comprobante'].isin(ALLOWED)].copy()

    # Formato final
    out = pd.DataFrame({
        'tipo_operacion': tipo_op,
        'fecha': pd.to_datetime(df['FECHA'], errors='coerce'),
        'tipo_comprobante': df['tipo_comprobante'],
        'NRO_FACTURA': df['NRO_FACTURA'],
        'CUIT': df['CUIT'],
        'Denominación': df[den],
        'PESOS_SIN_IVA': pd.to_numeric(df['PESOS_SIN_IVA'], errors='coerce').fillna(0.0),
        'IVA_21': pd.to_numeric(df['IVA_21'], errors='coerce').fillna(0.0),
        'IVA_105': pd.to_numeric(df['IVA_105'], errors='coerce').fillna(0.0),
        'TOTAL_CON_IVA': pd.to_numeric(df['TOTAL_CON_IVA'], errors='coerce').fillna(0.0),
        'estado': df['ESTADO'] if 'ESTADO' in df.columns else None,
        'origen_destino': df['ORIGEN_DESTINO'],
        'nombre_socio': df['nombre_socio'],
    })
    return out


def main(path, ym_filter=None):
    xls = pd.ExcelFile(path)
    compras = _read_sheet(xls, 'FactCompras')
    ventas  = _read_sheet(xls, 'FactVentas')

    rc = _std_cols(compras, 'COMPRA')
    rv = _std_cols(ventas,  'VENTA')

    df = pd.concat([rc, rv], ignore_index=True)
    if ym_filter:
        ym = str(ym_filter)
        df = df[df['fecha'].dt.strftime('%Y-%m') == ym]

    df = df.sort_values('fecha')
    out_resumen = Path('Resumen_ARCA_filtrado.csv')
    df.to_csv(out_resumen, index=False, encoding='utf-8')

    # Totales por YM y tipo
    tmp = df.copy()
    tmp['YM'] = tmp['fecha'].dt.strftime('%Y-%m')
    group = tmp.groupby(['YM','tipo_operacion']).agg(
        PESOS_SIN_IVA=('PESOS_SIN_IVA','sum'),
        IVA_21=('IVA_21','sum'),
        IVA_105=('IVA_105','sum'),
        TOTAL_CON_IVA=('TOTAL_CON_IVA','sum'),
    ).reset_index()

    # Saldo técnico por YM
    saldo = group.assign(IVA_TOTAL=group['IVA_21']+group['IVA_105'])
    saldo_v = saldo[saldo['tipo_operacion']=='VENTA'].groupby('YM')['IVA_TOTAL'].sum()
    saldo_c = saldo[saldo['tipo_operacion']=='COMPRA'].groupby('YM')['IVA_TOTAL'].sum()
    saldo_total = (saldo_v - saldo_c).fillna(0.0)

    group['Saldo_Tecnico_IVA'] = group['YM'].map(saldo_total)

    out_totales = Path('Totales_ARCA_filtrado.csv')
    group.to_csv(out_totales, index=False, encoding='utf-8')

    print(f"OK -> {out_resumen.resolve()}  |  {out_totales.resolve()}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python scripts/validar_arca_desde_excel.py archivo.xlsm [YYYY-MM]')
        sys.exit(1)
    path = sys.argv[1]
    ym = sys.argv[2] if len(sys.argv) > 2 else None
    main(path, ym)
