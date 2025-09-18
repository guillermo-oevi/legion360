# main.py — OEVI FULL (con dashboard_export corregido)
# Requisitos: flask, flask_sqlalchemy, pandas, openpyxl, requests

from datetime import date, datetime
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    Response,
    send_file,
    send_from_directory,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, literal_column, text
from werkzeug.utils import secure_filename

import os, io, csv, shutil, time, hashlib


def color_index(value, num_colors=8):
    import hashlib
    hash_val = hashlib.md5(str(value).encode()).hexdigest()
    return int(hash_val, 16) % num_colors


try:
    import pandas as pd
except Exception:
    pd = None

try:
    import requests
except Exception:
    requests = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "app.db")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
BACKUPS_FOLDER = os.path.join(BASE_DIR, "backups")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(BACKUPS_FOLDER, exist_ok=True)

app = Flask(__name__, template_folder='docs')
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + DB_PATH
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-in-prod")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
ALLOWED_XL = {".xlsx", ".xlsm", ".xls"}

# ID por defecto de Google Sheet
app.config["DEFAULT_GSHEET_ID"] = os.getenv(
    "DEFAULT_GSHEET_ID", "1M7BLBqPM3rzrniaekB_EEoaRZ-NDTFp0phFkObRP5Qw"
)

# ------------------- MODELOS -------------------
db = SQLAlchemy(app)


class Socio(db.Model):
    __tablename__ = "socios"
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), unique=True, nullable=False)
    tipo = db.Column(db.String(20), default="Socio")
    margen_porcentaje = db.Column(db.Float, nullable=True)


class Parametro(db.Model):
    __tablename__ = "parametros"
    clave = db.Column(db.String(100), primary_key=True)
    valor = db.Column(db.Float, nullable=False)


class Compra(db.Model):
    __tablename__ = "compras"
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    ym = db.Column(db.String(7), index=True)
    proveedor = db.Column(db.String(120))
    socio_id = db.Column(db.Integer, db.ForeignKey("socios.id"), nullable=True)
    pesos_sin_iva = db.Column(db.Float, default=0.0)
    iva_21 = db.Column(db.Float, default=0.0)
    iva_105 = db.Column(db.Float, default=0.0)
    total_con_iva = db.Column(db.Float, default=0.0)
    tipo = db.Column(db.String(5))
    nro_factura = db.Column(db.String(50))
    cuit = db.Column(db.String(20))
    origen = db.Column(db.String(50))
    estado = db.Column(db.String(20), default="PAGADO")
    descripcion = db.Column(db.String(255))
    personal = db.Column(db.Boolean, default=False)
    iva_deducible_pct = db.Column(db.Float, default=None)
    transaccion_id = db.Column(db.String(100), nullable=True, index=True)

class Venta(db.Model):
    __tablename__ = "ventas"
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    ym = db.Column(db.String(7), index=True)
    cliente = db.Column(db.String(120))
    socio_id = db.Column(db.Integer, db.ForeignKey("socios.id"), nullable=True)
    pesos_sin_iva = db.Column(db.Float, default=0.0)
    iva_21 = db.Column(db.Float, default=0.0)
    iva_105 = db.Column(db.Float, default=0.0)
    total_con_iva = db.Column(db.Float, default=0.0)
    nro_factura = db.Column(db.String(50))
    cuit_venta = db.Column(db.String(20))
    destino = db.Column(db.String(50))
    estado = db.Column(db.String(20), default="PAGADO")
    descripcion = db.Column(db.String(255))
    tipo = db.Column(db.String(5))
    transaccion_id = db.Column(db.String(100), nullable=True, index=True)

# ------------------- HELPERS -------------------


def parse_date(dstr: str):
    """
    Parsea una fecha desde distintos formatos comunes y devuelve un objeto datetime.date.

    Qué hace:
    - Intenta parsear la cadena `dstr` con formatos predefinidos ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y").
    - Si falla, intenta usar pandas (si está disponible) para aprovechar su heurística.
    - Lanza ValueError si no puede interpretar la entrada.

    Parámetros:
    - dstr: valor a parsear (str). Puede venir de Excel/CSV/Google Sheets.

    Devuelve:
    - datetime.date con la fecha parseada.

    Efectos secundarios / quién la consume:
    - Usada en la importación de Excel/Google Sheets (do_import_excel_from_path) para normalizar fechas.
    - También puede usarse en otros puntos del código que reciban fechas en texto.
    """
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(dstr).strip(), fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(dstr).date() if pd is not None else None
    except Exception:
        pass
    raise ValueError(f"Fecha inválida: {dstr}")


def ym_from_date(d: date):
    """
    Devuelve el periodo 'YYYY-MM' (string) a partir de un objeto datetime.date.

    Qué hace:
    - Construye y retorna el string en formato 'YYYY-MM' usado como clave de periodo en la BD.

    Parámetros:
    - d: datetime.date

    Devuelve:
    - str 'YYYY-MM'

    Quién la consume:
    - Usada en importación (do_import_excel_from_path) y al crear objetos Compra/Venta para fijar `ym`.
    """
    return f"{d.year:04d}-{d.month:02d}"


@app.template_filter("ars")
def format_ars(value, digits=2):
    """
    Formatea un número como moneda ARS para plantillas Jinja2.

    Qué hace:
    - Convierte el valor a float, aplica formato con separadores de miles y coma decimal.
    - Devuelve string con prefijo '$'.

    Parámetros:
    - value: valor numérico o convertible (float/int/None).
    - digits: decimales (int).

    Retorno:
    - str formateado (ej: "$1.234,56").

    Quién la consume:
    - Plantillas HTML usan este filtro para mostrar montos (p. ej. {{ monto|ars }}).
    """
    try:
        n = float(value or 0)
    except Exception:
        n = 0.0
    s = f"{n:,.{digits}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"${s}"


@app.template_filter("factnum")
def format_factnum(value):
    """
    Normaliza/da formato a números de factura para visualización.

    Qué hace:
    - Extrae solo dígitos del valor provisto.
    - Si hay hasta 8 dígitos asume punto de venta 1 y completa con ceros.
    - Si hay más, separa los primeros como punto de venta y los últimos 8 como número.
    - Devuelve 'PPPP-NNNNNNNN' (4 dígitos PV + '-' + 8 dígitos nro).

    Parámetros:
    - value: string / número con dígitos.

    Devuelve:
    - str con formato o cadena original si no se encuentran dígitos.

    Quién la consume:
    - Plantillas que muestran facturas (filtro en Jinja2).
    """
    if value is None:
        return ""
    s = str(value).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s
    if len(digits) <= 8:
        pv = "1"
        num = digits.zfill(8)
    else:
        pv = digits[:-8] or "1"
        num = digits[-8:].zfill(8)
    try:
        pv = str(int(pv))
    except Exception:
        pass
    pv_pad = pv.zfill(4)
    return f"{pv_pad}-{num}"


def get_param(clave: str, default: float | None = None) -> float:
    """
    Obtiene un parámetro desde la tabla Parametro o lo crea si no existe.

    Qué hace:
    - Consulta Parametro por clave.
    - Si existe devuelve su valor.
    - Si no existe y se pasó un default, crea el registro con ese valor y lo devuelve.
    - Si no existe y default es None, lanza RuntimeError.

    Parámetros:
    - clave: nombre de parámetro.
    - default: valor por defecto opcional.

    Devuelve:
    - float: valor del parámetro.

    Quién la consume:
    - Muchas vistas y builders usan parámetros (márgenes, porcentajes de IVA, flags).
    - Importante para comportamiento configurable sin tocar código.
    """
    p = db.session.get(Parametro, clave)
    if p is None:
        if default is None:
            raise RuntimeError(f"Parametro {clave} no encontrado y sin default")
        p = Parametro(clave=clave, valor=default)
        db.session.add(p)
        db.session.commit()
        return default
    return p.valor

def _read_param_any(keys, default=None):
    """
    Buscar un parámetro probando varias claves en orden y devolver su valor.
    - keys: lista de claves a probar (ej. ["margen_Empresa","margen_empresa"])
    - default: si se pasa y no existe ninguna clave, crea Parametro(clave=keys[0], valor=default)
               y devuelve default.

    Retorna:
    - valor del parámetro (float si es numérico) o lanza RuntimeError si no existe y default es None.
    """
    if not keys:
        raise ValueError("keys no puede ser vacío")
    for k in keys:
        # intentar por PK (si la clave es PK) y fallback a query por si no funciona
        try:
            p = db.session.get(Parametro, k)
        except Exception:
            p = None
        if p is None:
            p = db.session.query(Parametro).filter_by(clave=k).first()
        if p is not None:
            try:
                return float(p.valor)
            except Exception:
                return p.valor
    # si no existe ninguna clave, crear con default si se proporcionó
    if default is not None:
        k0 = keys[0]
        p_new = Parametro(clave=k0, valor=default)
        db.session.add(p_new)
        db.session.commit()
        try:
            return float(default)
        except Exception:
            return default
    raise RuntimeError(f"Ningún parametro encontrado para claves {keys} y sin default")


def build_resumen_socio(ym: str):
    """
    Construye un resumen de ventas/compras agregadas por socio para un periodo `ym`.

    Qué hace:
    - Lee parámetros de márgenes (empresa, vendedor, socio).
    - Crea consultas filtradas por `ym` (soporta 'all', 'none', 'YYYY-*' y 'YYYY-MM').
    - Genera subconsultas agregadas (ventas y compras por socio).
    - Junta con la tabla Socio para devolver lista de diccionarios con montos y márgenes.

    Parámetros:
    - ym: periodo string ('all', 'none', 'YYYY-*' o 'YYYY-MM').

    Devuelve:
    - (filas, p_emp, p_ven, p_soc)
      - filas: lista de dicts con claves: YM, nombre_socio, Ganancia_neta, Margen_Empresa, Margen_Vendedor, Margen_Socios, Margen_Otros_Socios, Total_Margenes
      - p_emp/p_ven/p_soc: valores de parámetros leídos.

    Quién la consume:
    - resumen_socio view (resumen por socio) y su export.
    - Usada para informes por socio y cálculo de márgenes.
    """
    # Lee parámetros (por clave) o usa fallback si no existen
    p_emp = _read_param_any(["margen_Empresa"], 0.53)
    p_ven = _read_param_any(["margen_Vendedor"], 0.20)
    p_soc = _read_param_any(["margen_Socio"], 0.09)

    # Construir ventas_query / compras_query según el valor de ym
    if ym == "all":
        compras_query = db.session.query(Compra)
        ventas_query = db.session.query(Venta)
    elif ym == "none" or not ym:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
    elif isinstance(ym, str) and ym.endswith("-*"):
        year_prefix = ym[:-2]  # "2025-*"[0:-2] => "2025"
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year_prefix}-%"))
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year_prefix}-%"))
    else:
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    # --- INICIO: CÁLCULO DE TOTALES POR CAJA ---
    # Se calcula el saldo de cada caja para el período filtrado.
    # Esta lógica es similar a la de `resumen_caja`.
    resumen_caja_temp = {}
    # Se usa la misma lógica de `resumen_caja` para calcular el egreso real.
    for c in compras_query.all():
        if not c.origen:
            continue
        # --- CÁLCULO DE EGRESO (COMPRA) ---
        pesos_sin_iva = float(c.pesos_sin_iva or 0.0)
        iva_total = float(c.iva_21 or 0.0) + float(c.iva_105 or 0.0)
        pct_deducible = float(c.iva_deducible_pct if c.iva_deducible_pct is not None else 1.0)
        iva_no_deducible = iva_total * (1 - pct_deducible)
        # El Gasto Real es el neto más el IVA que no se recupera.
        monto_gasto_real = pesos_sin_iva + iva_no_deducible
        # Se guarda como un monto negativo (egreso).
        resumen_caja_temp.setdefault(c.origen, []).append({"monto": -monto_gasto_real})

    # Se usa la misma lógica de `resumen_caja` para calcular el ingreso.
    for v in ventas_query.all():
        if not v.destino:
            continue
        # --- CÁLCULO DE INGRESO (VENTA) ---
        # El monto de ingreso es el total de la factura.
        monto_venta = float(v.total_con_iva or (v.pesos_sin_iva or 0.0) + (v.iva_21 or 0.0) + (v.iva_105 or 0.0))
        # Se guarda como un monto positivo (ingreso).
        resumen_caja_temp.setdefault(v.destino, []).append({"monto": monto_venta})

    totales_caja = {
        caja: round(sum(item["monto"] for item in movimientos), 2)
        for caja, movimientos in resumen_caja_temp.items()
    }
    # --- FIN: CÁLCULO DE TOTALES POR CAJA ---

    # Subconsultas de ventas/compras por socio (usando las queries ya filtradas)
    ventas_sub = (
        ventas_query.with_entities(
            Venta.socio_id.label("socio_id"),
            func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0).label("ventas_sin_iva"),
        )
        .group_by(Venta.socio_id)
        .subquery()
    )

    compras_sub = (
        compras_query.with_entities(
            Compra.socio_id.label("socio_id"),
            func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0).label("compras_sin_iva"),
        )
        .group_by(Compra.socio_id)
        .subquery()
    )

    # Traer id, nombre, tipo + montos agregados
    q = (
        db.session.query(
            Socio.id.label("id"),
            Socio.nombre.label("nombre"),
            Socio.tipo.label("tipo"),
            func.coalesce(ventas_sub.c.ventas_sin_iva, 0.0).label("ventas_sin_iva"),
            func.coalesce(compras_sub.c.compras_sin_iva, 0.0).label("compras_sin_iva"),
        )
        .outerjoin(ventas_sub, ventas_sub.c.socio_id == Socio.id)
        .outerjoin(compras_sub, compras_sub.c.socio_id == Socio.id)
    )

    # Normalizar resultados en una lista de dicts
    socios = []
    for sid, nombre, tipo, v_sin, c_sin in q.all():
        v = float(v_sin or 0.0)
        c = float(c_sin or 0.0)
        gn = v - c
        socios.append(
            {
                "id": sid,
                "nombre": nombre,
                "tipo": tipo,  # se asume Socio.tipo existe y es 'Socio' o 'Empresa'
                "ventas_sin_iva": v,
                "compras_sin_iva": c,
                "gn": gn,
            }
        )

    # Construcción de filas de salida + márgenes
    filas = []
    for s in socios:
        gn = s["gn"]
        tipo = s["tipo"]

        margen_empresa = round(gn * p_emp, 2)
        margen_vendedor = round(gn * p_ven, 2)
        margen_socio = round(gn * p_soc, 2) if tipo == "Socio" else 0.0

        # Acumulado de otros socios (según tu lógica original)
        margen_otros = 0.0
        for o in socios:
            if o["id"] == s["id"]:
                continue
            if tipo == "Socio" and o["tipo"] == "Socio":
                margen_otros += round(o["gn"] * p_soc, 2)
            elif tipo == "Empresa" and o["tipo"] == "Socio":
                margen_otros += round(o["gn"] * p_emp, 2)

        total_margenes = round(margen_vendedor + margen_socio + margen_otros, 2)
        total_caja = totales_caja.get(s["nombre"], 0.0)
        socio_nombre = s["nombre"]

        resto_calculado = 0.0
        if socio_nombre != "Legion":
            # Para socios que no son "Legion", si la caja es negativa, se usa su valor absoluto para el cálculo.
            caja_para_calculo = abs(total_caja)
            resto_calculado = round(caja_para_calculo - total_margenes, 2)
        else:
            # Para "Legion", se mantiene la lógica original.
            resto_calculado = round(total_caja - total_margenes, 2)

        filas.append(
            {
                "YM": ym,
                "nombre_socio": socio_nombre,
                "Ganancia_neta": round(gn, 2),
                "Margen_Empresa": margen_empresa,
                "Margen_Vendedor": margen_vendedor,
                "Margen_Socios": margen_socio,
                "Margen_Otros_Socios": round(margen_otros, 2),
                "Total_Margenes": total_margenes,
                "Total_Caja": total_caja,
                "Resto": resto_calculado,
            }
        )

    return filas, p_emp, p_ven, p_soc


# ------------------- ARCA -------------------


def _split_fact(nro_raw):
    """
    Separa un número de factura crudo en punto de venta (4 dígitos), número (8 dígitos) y formato 'PV-NRO'.

    Qué hace:
    - Extrae dígitos, determina punto de venta y el número (padding si es necesario).

    Parámetros:
    - nro_raw: cadena o número con dígitos.

    Devuelve:
    - (pv_pad, num, f"{pv_pad}-{num}").

    Quién la consume:
    - build_resumen_arca para mostrar / exportar facturas con formato consistente.
    """
    s = "".join(ch for ch in str(nro_raw or "").strip() if ch.isdigit())
    if not s:
        return "", "", ""
    if len(s) <= 8:
        pv = "1"
        num = s.zfill(8)
    else:
        pv = s[:-8] or "1"
        num = s[-8:].zfill(8)
    try:
        pv = str(int(pv))
    except Exception:
        pass
    pv_pad = pv.zfill(4)
    return pv_pad, num, f"{pv_pad}-{num}"


def build_resumen_arca():
    """
    Construye la lista 'plana' de operaciones ARCA (compras + ventas) para mostrar en Resumen ARCA.

    Qué hace:
    - Lee todas las compras y ventas y construye una fila por operación con campos normalizados:
      tipo_operacion, fecha, tipo_comprobante, NRO_FACTURA, PUNTO_VENTA, NRO_COMPROBANTE, CUIT, Denominación,
      PESOS_SIN_IVA, IVA_21, IVA_105, TOTAL_CON_IVA, estado, origen_destino, nombre_socio.
    - Calcula TOTAL_CON_IVA por fila haciendo fallback a (pesos_sin_iva + iva_21 + iva_105) cuando total_con_iva está en 0 o NULL.
    - Normaliza nombres de socio consultando la tabla Socio.

    Retorna:
    - lista de diccionarios (filas) que consumen las vistas resumen_arca, totales_arca y sus exportadores.

    Quién la consume:
    - resumen_arca view / export
    - totales_arca view / export (a través de build_totales_arca)
    - Herramientas de depuración / exports
    """
    filas = []
    socios_map = {
        sid: nom for sid, nom in db.session.query(Socio.id, Socio.nombre).all()
    }
    for c in db.session.query(Compra).all():
        tipo_up = (c.tipo or "").strip().upper()
        pv, nro8, nro_fmt = _split_fact(c.nro_factura)

        # calcular total c/iva con fallback cuando total_con_iva es 0 o None
        total_civa = None
        try:
            if c.total_con_iva is not None and float(c.total_con_iva) != 0.0:
                total_civa = float(c.total_con_iva)
            else:
                total_civa = float((c.pesos_sin_iva or 0.0) + (c.iva_21 or 0.0) + (c.iva_105 or 0.0))
        except Exception:
            total_civa = float((c.pesos_sin_iva or 0.0) + (c.iva_21 or 0.0) + (c.iva_105 or 0.0))

        filas.append(
            {
                "tipo_operacion": "COMPRA",
                "fecha": c.fecha.strftime("%Y-%m-%d"),
                "tipo_comprobante": tipo_up,
                "NRO_FACTURA": c.nro_factura or "",
                "NRO_FACTURA_FMT": nro_fmt,
                "PUNTO_VENTA": pv,
                "NRO_COMPROBANTE": nro8,
                "CUIT": c.cuit or "",
                "Denominación": c.proveedor or "",
                "PESOS_SIN_IVA": round(c.pesos_sin_iva or 0.0, 2),
                "IVA_21": round(c.iva_21 or 0.0, 2),
                "IVA_105": round(c.iva_105 or 0.0, 2),
                "TOTAL_CON_IVA": round(total_civa or 0.0, 2),
                "estado": c.estado or "",
                "origen_destino": c.origen or "",
                "nombre_socio": socios_map.get(c.socio_id, ""),
            }
        )
    for v in db.session.query(Venta).all():
        tipo_up = (v.tipo or "").strip().upper()
        pv, nro8, nro_fmt = _split_fact(v.nro_factura)

        # calcular total c/iva con fallback cuando total_con_iva es 0 o None (ventas)
        total_v_civa = None
        try:
            if v.total_con_iva is not None and float(v.total_con_iva) != 0.0:
                total_v_civa = float(v.total_con_iva)
            else:
                total_v_civa = float((v.pesos_sin_iva or 0.0) + (v.iva_21 or 0.0) + (v.iva_105 or 0.0))
        except Exception:
            total_v_civa = float((v.pesos_sin_iva or 0.0) + (v.iva_21 or 0.0) + (v.iva_105 or 0.0))

        filas.append(
            {
                "tipo_operacion": "VENTA",
                "fecha": v.fecha.strftime("%Y-%m-%d"),
                "tipo_comprobante": tipo_up,
                "NRO_FACTURA": v.nro_factura or "",
                "NRO_FACTURA_FMT": nro_fmt,
                "PUNTO_VENTA": pv,
                "NRO_COMPROBANTE": nro8,
                "CUIT": v.cuit_venta or "",
                "Denominación": v.cliente or "",
                "PESOS_SIN_IVA": round(v.pesos_sin_iva or 0.0, 2),
                "IVA_21": round(v.iva_21 or 0.0, 2),
                "IVA_105": round(v.iva_105 or 0.0, 2),
                "TOTAL_CON_IVA": round(total_v_civa or 0.0, 2),
                "estado": v.estado or "",
                "origen_destino": v.destino or "",
                "nombre_socio": socios_map.get(v.socio_id, ""),
            }
        )
    return filas


def build_totales_arca(filtered=None):
    """
    Agrega (suma) las filas de build_resumen_arca por periodo (YM) y tipo_operacion.

    Qué hace:
    - Acepta 'filtered' (lista de filas ya filtradas) o genera todas las filas.
    - Agrupa por (YM, tipo_operacion) y suma PESOS_SIN_IVA, IVA_21, IVA_105 y TOTAL_CON_IVA.
    - Añade cálculo Saldo_Tecnico_IVA = IVA_21 + IVA_105 y redondea resultados.

    Parámetros:
    - filtered: lista opcional de filas (tal como las generadas por build_resumen_arca).

    Devuelve:
    - lista de diccionarios con claves: YM, tipo_operacion, PESOS_SIN_IVA, IVA_21, IVA_105, TOTAL_CON_IVA, Saldo_Tecnico_IVA.

    Quién la consume:
    - totales_arca view y su export. Garantiza el formato que usan las plantillas.
    """
    filas = filtered if filtered is not None else build_resumen_arca()
    agg = {}
    for f in filas:
        ym = f["fecha"][:7]
        key = (ym, f["tipo_operacion"])
        d = agg.setdefault(
            key,
            {
                "YM": ym,
                "tipo_operacion": f["tipo_operacion"],
                "PESOS_SIN_IVA": 0.0,
                "IVA_21": 0.0,
                "IVA_105": 0.0,
                "TOTAL_CON_IVA": 0.0,
            },
        )
        d["PESOS_SIN_IVA"] += f["PESOS_SIN_IVA"]
        d["IVA_21"] += f["IVA_21"]
        d["IVA_105"] += f["IVA_105"]
        d["TOTAL_CON_IVA"] += f["TOTAL_CON_IVA"]
    filas_out = []
    for d in agg.values():
        d["Saldo_Tecnico_IVA"] = round(d["IVA_21"] + d["IVA_105"], 2)
        d["PESOS_SIN_IVA"] = round(d["PESOS_SIN_IVA"], 2)
        d["IVA_21"] = round(d["IVA_21"], 2)
        d["IVA_105"] = round(d["IVA_105"], 2)
        d["TOTAL_CON_IVA"] = round(d["TOTAL_CON_IVA"], 2)
        filas_out.append(d)
    filas_out.sort(key=lambda x: (x["YM"], x["tipo_operacion"]))
    return filas_out


# ------------------- RUTAS -------------------
@app.route("/")
def index():
    """
    Página principal / dashboard.

    Qué hace:
    - Lee filtros year/month de la querystring y construye consultas filtradas.
    - Calcula totales agregados (ventas sin IVA, IVA ventas, compras sin IVA, IVA compras).
    - Calcula IVA deducible considerando la bandera 'personal' y porcentajes configurables.
    - Prepara datos por socio para mostrar en el dashboard.
    - Renderiza 'index.html' con todos los totales y listas auxiliares.

    Parámetros (via querystring):
    - year: año (int) (opcional)
    - month: mes (1-12) o 13 para 'Todos' (opcional)

    Renderiza:
    - 'index.html' con variables: ventas_tot, compras_tot, ventas_sin_iva, compras_sin_iva, ganancia_neta, iva_a_pagar, per_socio, current_year, etc.

    Quién la consume:
    - Usuario final a través del navegador.
    """
    today = date.today()
    year = int(request.args.get("year", today.year))

    # Si no se recibe 'month' en la query, por defecto usamos 13 -> "Todos"
    month_arg = request.args.get("month", None)
    month = int(month_arg) if month_arg is not None else 13

    if year == 1313 and month == 13:
        compras_query = db.session.query(Compra)
        ventas_query = db.session.query(Venta)
        ym = "all"
    elif month == 13:
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
        ym = f"{year}-*"
    elif year == 1313:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
        ym = "none"
    else:
        ym = f"{year:04d}-{month:02d}"
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    v = ventas_query.with_entities(
        func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Venta.iva_21 + Venta.iva_105), 0.0),
    ).first()
    ventas_sin_iva, iva_venta = float(v[0]), float(v[1])

    c = compras_query.with_entities(
        func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Compra.iva_21 + Compra.iva_105), 0.0),
    ).first()
    compras_sin_iva, iva_compra_total = float(c[0]), float(c[1])

    # usar compras_query para calcular IVA personal (respeta filtros "all"/"year-*")
    iva_personal_total = float(
        compras_query.with_entities(
            func.coalesce(func.sum(Compra.iva_21 + Compra.iva_105), 0.0)
        )
        .filter(Compra.personal == True)
        .scalar()
        or 0.0
    )

    p_norm = get_param("iva_deducible_normal_pct", 1.0)
    p_pers_def = get_param("iva_deducible_personal_default_pct", 0.5)

    # obtener filas desde compras_query para respetar filtros
    rows = compras_query.with_entities(
        Compra.personal, Compra.iva_deducible_pct, Compra.iva_21, Compra.iva_105
    ).all()

    iva_compra_creditable = 0.0
    iva_personal_credito_empresa = 0.0
    for personal, pct, i21, i105 in rows:
        base = float((i21 or 0.0) + (i105 or 0.0))
        eff = float(pct if pct is not None else (p_pers_def if personal else p_norm))
        eff = min(max(eff, 0.0), 1.0)
        iva_compra_creditable += base * eff
        if personal:
            iva_personal_credito_empresa += base * eff

    margen_sin_iva = ventas_sin_iva - compras_sin_iva
    iva_a_pagar = iva_venta - iva_compra_creditable
    adeudado_compras = (
        db.session.query(func.count(Compra.id))
        .filter((Compra.ym == ym) & (Compra.estado == "ADEUDADO"))
        .scalar()
    )
    adeudado_ventas = (
        db.session.query(func.count(Venta.id))
        .filter((Venta.ym == ym) & (Venta.estado == "ADEUDADO"))
        .scalar()
    )

    # Contar ADEUDADOS reutilizando compras_query/ventas_query (respetan filtros "all"/"year-*"/"none")
    adeudado_compras = int(
        compras_query.with_entities(func.count(Compra.id))
        .filter(Compra.estado == "ADEUDADO")
        .scalar()
        or 0
    )
    adeudado_ventas = int(
        ventas_query.with_entities(func.count(Venta.id))
        .filter(Venta.estado == "ADEUDADO")
        .scalar()
        or 0
    )

    # Subconsultas por socio: usar ventas_query/compras_query SIN volver a filtrar por ym
    ventas_sub = (
        ventas_query.with_entities(
            Venta.socio_id.label("socio_id"),
            func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0).label("ventas_sin_iva"),
        )
        .group_by(Venta.socio_id)
        .subquery()
    )
    compras_sub = (
        compras_query.with_entities(
            Compra.socio_id.label("socio_id"),
            func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0).label("compras_sin_iva"),
        )
        .group_by(Compra.socio_id)
        .subquery()
    )
    q = (
        db.session.query(
            Socio.nombre,
            func.coalesce(ventas_sub.c.ventas_sin_iva, 0.0),
            func.coalesce(compras_sub.c.compras_sin_iva, 0.0),
        )
        .outerjoin(ventas_sub, ventas_sub.c.socio_id == Socio.id)
        .outerjoin(compras_sub, compras_sub.c.socio_id == Socio.id)
    )
    per_socio = [
        {
            "nombre": nombre,
            "ventas_sin_iva": float(v_sin or 0.0),
            "compras_sin_iva": float(c_sin or 0.0),
            "ganancia_neta": float((v_sin or 0.0) - (c_sin or 0.0)),
        }
        for nombre, v_sin, c_sin in q.all()
    ]

    return render_template(
        "index.html",
        year=year,
        month=month,
        ventas_tot={"monto_total": ventas_sin_iva + iva_venta, "iva": iva_venta},
        compras_tot={
            "monto_total": compras_sin_iva + iva_compra_total,
            "iva": iva_compra_total,
            "iva_deducible": iva_compra_creditable,
        },
        ventas_sin_iva=ventas_sin_iva,
        compras_sin_iva=compras_sin_iva,
        ganancia_neta=margen_sin_iva,
        iva_a_pagar=iva_a_pagar,
        iva_personal_total=iva_personal_total,
        iva_personal_credito_empresa=iva_personal_credito_empresa,
        iva_personal_credito_socios=max(
            iva_personal_total - iva_personal_credito_empresa, 0.0
        ),
        adeudado_compras=adeudado_compras,
        adeudado_ventas=adeudado_ventas,
        per_socio=per_socio,
        debug=True,
        current_year=today.year,
    )


# ------------------- Dashboard export (RESTABLECIDO) -------------------
@app.route("/dashboard/export")
def dashboard_export():
    """
    Exporta un resumen dashboard (por year/month) en CSV o XLSX.

    Qué hace:
    - Aplica la misma lógica de filtros year/month que la vista index.
    - Calcula varios agregados (ventas, compras, IVA personal, adeudados).
    - Devuelve un XLSX o CSV con un único registro resumen.

    Parámetros (querystring):
    - year, month: para filtrar el periodo.
    - format: 'csv' (default) o 'xlsx'.

    Quién la consume:
    - Usuario a través de la UI (botón Exportar en el dashboard).
    """
    today = date.today()
    year = int(request.args.get("year", today.year))
    month = int(request.args.get("month", today.month))

    if year == 1313 and month == 13:
        compras_query = db.session.query(Compra)
        ventas_query = db.session.query(Venta)
        ym = "all"
    elif month == 13:
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
        ym = f"{year}-*"
    elif year == 1313:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
        ym = "none"
    else:
        ym = f"{year:04d}-{month:02d}"
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    v = ventas_query.with_entities(
        func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Venta.iva_21 + Venta.iva_105), 0.0),
    ).first()
    ventas_sin_iva, iva_venta = float(v[0]), float(v[1])

    c = compras_query.with_entities(
        func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Compra.iva_21 + Compra.iva_105), 0.0),
    ).first()
    compras_sin_iva, iva_compra_total = float(c[0]), float(c[1])

    p_norm = get_param("iva_deducible_normal_pct", 1.0)
    p_pers_def = get_param("iva_deducible_personal_default_pct", 0.5)
    rows = (
        db.session.query(
            Compra.personal, Compra.iva_deducible_pct, Compra.iva_21, Compra.iva_105
        )
        .filter(Compra.ym == ym)
        .all()
    )

    iva_compra_creditable, iva_personal_total, iva_personal_credito_empresa = (
        0.0,
        0.0,
        0.0,
    )
    for personal, pct, i21, i105 in rows:
        base = float((i21 or 0.0) + (i105 or 0.0))
        eff = float(pct if pct is not None else (p_pers_def if personal else p_norm))
        eff = min(max(eff, 0.0), 1.0)
        iva_compra_creditable += base * eff
        if personal:
            iva_personal_total += base
            iva_personal_credito_empresa += base * eff

    resumen = [
        {
            "YM": ym,
            "Ventas_sin_IVA": round(ventas_sin_iva, 2),
            "IVA_Venta": round(iva_venta, 2),
            "Compras_sin_IVA": round(compras_sin_iva, 2),
            "IVA_Compra": round(iva_compra_total, 2),
            "IVA_Personal_Total": round(iva_personal_total, 2),
            "IVA_Personal_Creditable": round(iva_personal_credito_empresa, 2),
            "IVA_Compra_Creditable": round(iva_compra_creditable, 2),
            "Margen_sin_IVA": round(margen_sin_iva, 2),
            "IVA_a_Pagar": round(iva_a_pagar, 2),
            "Compras_ADEUDADO": int(
                compras_query.with_entities(func.count(Compra.id))
                .filter(Compra.estado == "ADEUDADO")
                .scalar()
                or 0
            ),
            "Ventas_ADEUDADO": int(
                ventas_query.with_entities(func.count(Venta.id))
                .filter(Venta.estado == "ADEUDADO")
                .scalar()
                or 0
            ),
        }
    ]

    fmt = request.args.get("format", "csv").lower()
    if fmt == "xlsx":
        if pd is None:
            return Response("Pandas no instalado", status=500)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            pd.DataFrame(resumen).to_excel(
                writer, index=False, sheet_name=f"Resumen_{ym}"
            )
        bio.seek(0)
        return send_file(
            bio,
            as_attachment=True,
            download_name=f"dashboard_{ym}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        sio = io.StringIO()
        writer = csv.DictWriter(sio, fieldnames=resumen[0].keys())
        writer.writeheader()
        writer.writerow(resumen[0])
        data = sio.getvalue()
        return Response(
            data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=dashboard_{ym}.csv"},
        )


# --------- Rutas ARCA / Socio / Import / Limpieza / Listas (igual que anteriores) ---------
@app.route("/resumen-arca")
def resumen_arca():
    filas = build_resumen_arca()
    ym = request.args.get("ym")
    tipo = (request.args.get("tipo") or "").upper()
    incluirN = request.args.get("incluirN", "0") == "1"
    if ym:
        filas = [f for f in filas if f["fecha"].startswith(ym)]
    if not incluirN:
        filas = [f for f in filas if (f["tipo_comprobante"] in {"A", "B"})]
    if tipo in {"A", "B", "N"}:
        filas = [f for f in filas if f["tipo_comprobante"] == tipo]
    all_dates = sorted({f["fecha"][:7] for f in build_resumen_arca()})
    return render_template(
        "resumen_arca.html",
        filas=filas,
        ym=ym or "",
        ym_list=all_dates,
        tipo=tipo or "",
        incluirN=int(incluirN),
    )


@app.route("/resumen-arca/export")
def resumen_arca_export():
    filas = build_resumen_arca()
    ym = request.args.get("ym")
    tipo = (request.args.get("tipo") or "").upper()
    incluirN = request.args.get("incluirN", "0") == "1"
    fmt = request.args.get("format", "csv").lower()
    if ym:
        filas = [f for f in filas if f["fecha"].startswith(ym)]
    if not incluirN:
        filas = [f for f in filas if (f["tipo_comprobante"] in {"A", "B"})]
    if tipo in {"A", "B", "N"}:
        filas = [f for f in filas if f["tipo_comprobante"] == tipo]
    if fmt == "xlsx":
        if pd is None:
            return Response("Pandas no instalado", status=500)
        df = pd.DataFrame(filas)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Resumen_ARCA")
        bio.seek(0)
        name = f"resumen_arca_{ym or 'all'}{('_'+tipo) if tipo else ''}{'_inclN' if incluirN else ''}.xlsx"
        return send_file(
            bio,
            as_attachment=True,
            download_name=name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        sio = io.StringIO()
        if filas:
            writer = csv.DictWriter(sio, fieldnames=filas[0].keys())
            writer.writeheader()
            writer.writerows(filas)
        return Response(
            sio.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=resumen_arca_{ym or 'all'}{('_'+tipo) if tipo else ''}{'_inclN' if incluirN else ''}.csv"
            },
        )

@app.route("/resumen-caja/export")
def resumen_caja_export():
    year = int(request.args.get("year", date.today().year))
    month = int(request.args.get("month", 13))
    caja_filtro = request.args.get("caja", "").strip()
    fmt = request.args.get("format", "csv").lower()

    if year == 1313 and month == 13:
        compras_query = db.session.query(Compra)
        ventas_query = db.session.query(Venta)
    elif month == 13:
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
    elif year == 1313:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
    else:
        ym = f"{year:04d}-{month:02d}"
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    if caja_filtro:
        compras_query = compras_query.filter(Compra.origen == caja_filtro)
        ventas_query = ventas_query.filter(Venta.destino == caja_filtro)

    rows = []
    for c in compras_query.all():
        if not c.origen:
            continue
        rows.append({
            "Caja": c.origen,
            "Fecha": c.fecha.strftime("%Y-%m-%d"),
            "Tipo": "COMPRA",
            "Detalle": c.descripcion,
            "Monto": -float(c.total_con_iva or 0.0)
        })
    for v in ventas_query.all():
        if not v.destino:
            continue
        rows.append({
            "Caja": v.destino,
            "Fecha": v.fecha.strftime("%Y-%m-%d"),
            "Tipo": "VENTA",
            "Detalle": v.descripcion,
            "Monto": float(v.total_con_iva or 0.0)
        })

    if fmt == "xlsx":
        bio = io.BytesIO()
        df = pd.DataFrame(rows)
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="ResumenCaja")
        bio.seek(0)
        return send_file(bio, as_attachment=True, download_name="resumen_caja.xlsx",
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        sio = io.StringIO()
        writer = csv.DictWriter(sio, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return Response(sio.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=resumen_caja.csv"})


@app.route("/resumen-caja")
def resumen_caja():
    today = date.today()
    year = int(request.args.get("year", today.year))
    month = int(request.args.get("month", 13))
    caja_filtro = request.args.get("caja", "").strip()
    transaccion_id_filtro = request.args.get("transaccion_id", "").strip()

    # Obtener todos los IDs de transacción únicos para el menú de filtro
    compra_tids = db.session.query(Compra.transaccion_id).filter(Compra.transaccion_id.isnot(None)).distinct()
    venta_tids = db.session.query(Venta.transaccion_id).filter(Venta.transaccion_id.isnot(None)).distinct()
    transacciones_unicas = sorted({tid[0] for tid in compra_tids.union(venta_tids).all() if tid[0]})

    if year == 1313 and month == 13:
        compras_query = db.session.query(Compra)
        ventas_query = db.session.query(Venta)
    elif month == 13:
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
    elif year == 1313:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
    else:
        ym = f"{year:04d}-{month:02d}"
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    # Aplicar filtro de transacción si se proporciona uno
    if transaccion_id_filtro:
        compras_query = compras_query.filter(Compra.transaccion_id == transaccion_id_filtro)
        ventas_query = ventas_query.filter(Venta.transaccion_id == transaccion_id_filtro)

    if caja_filtro:
        compras_query = compras_query.filter(Compra.origen == caja_filtro)
        ventas_query = ventas_query.filter(Venta.destino == caja_filtro)

    resumen = {}
    cajas = set()

    for c in compras_query.all():
        if not c.origen:
            continue
        cajas.add(c.origen)
        # --- CÁLCULO DE EGRESO (COMPRA) ---
        # 1. Se toman los valores base de la compra.
        pesos_sin_iva = float(c.pesos_sin_iva or 0.0)
        iva_total = float(c.iva_21 or 0.0) + float(c.iva_105 or 0.0)
        
        # 2. Se determina el porcentaje de IVA que se puede usar como crédito fiscal.
        #    `iva_deducible_pct` viene del Excel (ej: 0.5 para 50%). Si no está, se usan los defaults.
        pct_deducible = float(c.iva_deducible_pct if c.iva_deducible_pct is not None else 1.0)
        
        # 3. Se calcula la porción del IVA que NO es crédito fiscal y, por lo tanto, es un gasto.
        iva_no_deducible = iva_total * (1 - pct_deducible)

        # 4. El "Gasto Real" es el neto más el IVA que no se recupera. Este es el dinero que efectivamente sale de la caja.
        monto_gasto_real = pesos_sin_iva + iva_no_deducible
        
        
        resumen.setdefault(c.origen, []).append({
            "tipo": "COMPRA",
            "fecha": c.fecha,
            "detalle": c.descripcion,
            # 5. El monto se guarda en NEGATIVO para representar una salida de dinero (egreso).
            "monto": -monto_gasto_real,
            "transaccion_id": c.transaccion_id,
            "personal": c.personal
        })



    for v in ventas_query.all():
        if not v.destino:
            continue
        cajas.add(v.destino)
        # --- CÁLCULO DE INGRESO (VENTA) ---
        # 1. Se toma el monto total de la factura de venta.
        #    Si `total_con_iva` es 0 o nulo, se calcula sumando el neto más los IVAs como fallback.
        monto_venta = float(v.total_con_iva or (v.pesos_sin_iva or 0.0) + (v.iva_21 or 0.0) + (v.iva_105 or 0.0))
        
        resumen.setdefault(v.destino, []).append({
        "tipo": "VENTA",
        "fecha": v.fecha,
        "detalle": v.descripcion,
        # 2. El monto se guarda en POSITIVO para representar una entrada de dinero (ingreso).
        "monto": monto_venta,
        "transaccion_id": v.transaccion_id,
        "personal": False
        })



# Ordenar los movimientos por transaccion_id (asc) y luego por fecha (desc)
    for movimientos in resumen.values():
        # 1. Sort by date descending (secondary sort)
        movimientos.sort(key=lambda x: x["fecha"], reverse=True)
        # 2. Sort by transaction_id ascending (primary sort). Python's sort is stable.
        #    We use a very high unicode character for None/empty IDs to push them to the end.
        movimientos.sort(key=lambda x: x.get("transaccion_id") or '\uffff')

    # Lógica para asignar colores por transaccion_id
    transaccion_color_map = {}
    color_idx_counter = 0
    all_movimientos = [mov for caja_movs in resumen.values() for mov in caja_movs]

    for m in all_movimientos:
        tid = m.get("transaccion_id")
        # Solo asignamos un índice de color si existe un transaccion_id no vacío.
        # De lo contrario, será None y la plantilla no aplicará color,
        # permitiendo que funcione el estilo de filas alternas (striped).
        if tid:
            # Usamos la función de hash para obtener un índice de color determinista.
            m['color_index'] = color_index(m['transaccion_id'], 8)
        else:
            m['color_index'] = None

    # --- CÁLCULO DEL TOTAL POR CAJA ---
    # Aquí se calcula el saldo final para cada caja.
    # Se itera sobre cada 'caja' en el diccionario 'resumen'.
    # Para cada caja, se suman todos los valores de 'monto' de sus movimientos.
    #   - Las ventas tienen un 'monto' positivo (ingreso).
    #   - Las compras tienen un 'monto' negativo (egreso).
    # El resultado es el balance neto de la caja para el período filtrado.
    totales = {
        caja: round(sum(item["monto"] for item in resumen[caja]), 2)
        for caja in resumen
    }

    return render_template(
        "resumen_caja.html",
        resumen=resumen,
        totales=totales,
        cajas=sorted(cajas),
        caja_filtro=caja_filtro,
        year=year,
        month=month,
        transacciones_unicas=transacciones_unicas,
        transaccion_id=transaccion_id_filtro,
        current_year=today.year,
        months={
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
    )



@app.route("/totales-arca")
def totales_arca():
    ym = request.args.get("ym", "") or ""
    tipo = (request.args.get("tipo") or "").upper()
    incluirN = request.args.get("incluirN", "0") == "1"

    try:
        filas_totales = build_totales_arca()
    except Exception as e:
        print("[totales_arca] build_resumen_arca error:", e)
        filas_totales = []

    def valid_ym(f):
        y = f.get("YM", "") or ""
        if not (isinstance(y, str) and len(y) == 7 and y[4] == '-' and y[:4].isdigit()):
            return False
        if y == "1970-01":
            return False
        return True

    filas = [f for f in filas_totales if valid_ym(f)]
    if ym:
        filas = [f for f in filas if f.get("YM", "") == ym]

    filas = sorted(filas, key=lambda x: (x.get("YM", ""), x.get("tipo_operacion", "")), reverse=False)
    ym_list = sorted({f.get("YM", "") for f in filas if f.get("YM")}, reverse=True)

    # Totales por YM: resultado = Saldo_Tecnico_IVA(VENTA) - Saldo_Tecnico_IVA(COMPRA)
    totals = []
    for y in sorted({f.get("YM") for f in filas}):
        ventas = sum((f.get("Saldo_Tecnico_IVA") or 0) for f in filas if f.get("YM") == y and (f.get("tipo_operacion") or "").upper() == "VENTA")
        compras = sum((f.get("Saldo_Tecnico_IVA") or 0) for f in filas if f.get("YM") == y and (f.get("tipo_operacion") or "").upper() == "COMPRA")
        totals.append({"YM": y, "ventas": ventas, "compras": compras, "resultado": ventas - compras})

    print(f"[totales_arca] pasando {len(filas)} filas a template (raw totales={len(filas_totales)}) totals={len(totals)}")

    return render_template(
        "totales_arca.html",
        filas=filas,
        ym=ym or "",
        ym_list=ym_list,
        tipo=tipo or "",
        incluirN=int(incluirN),
        totals=totals,
    )


@app.route("/totales-arca/export")
def totales_arca_export():
    filas = build_resumen_arca()
    ym = request.args.get("ym")
    tipo = (request.args.get("tipo") or "").upper()
    incluirN = request.args.get("incluirN", "0") == "1"
    fmt = request.args.get("format", "csv").lower()
    if ym:
        filas = [f for f in filas if f["fecha"][:7] == ym]
    if not incluirN:
        filas = [f for f in filas if (f["tipo_comprobante"] in {"A", "B"})]
    if tipo in {"A", "B", "N"}:
        filas = [f for f in filas if f["tipo_comprobante"] == tipo]

    # usar el agregador existente (asegura keys/format compatibles con la plantilla)
    filas_totales = build_totales_arca(filtered=filas)

    if fmt == "xlsx":
        if pd is None:
            return Response("Pandas no instalado", status=500)
        df = pd.DataFrame(filas_totales)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Totales_ARCA")
        bio.seek(0)
        name = f"totales_arca_{ym or 'all'}{('_'+tipo) if tipo else ''}{'_inclN' if incluirN else ''}.xlsx"
        return send_file(
            bio,
            as_attachment=True,
            download_name=name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        sio = io.StringIO()
        if filas_totales:
            writer = csv.DictWriter(sio, fieldnames=filas_totales[0].keys())
            writer.writeheader()
            writer.writerows(filas_totales)
        return Response(
            sio.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=totales_arca_{ym or 'all'}{('_'+tipo) if tipo else ''}{'_inclN' if incluirN else ''}.csv"
            },
        )


@app.route("/resumen-socio", endpoint="resumen_socio")
def resumen_socio_view():
    """
    Resumen por socio con filtros year/month (month 1-12, 13 = Todos).
    Compatibilidad:
      - Si se pasan year/month se usan para construir ym (same semantics as index/ventas/compras).
      - Si se pasa ym (legacy) y no se pasan year/month, se respeta el comportamiento anterior.
    Pasa 'year' y 'month' al template para que los selects puedan mostrarlos.
    """
    # lista de YMs disponibles (legacy)
    yms = db.session.query(Compra.ym).distinct().union(db.session.query(Venta.ym)).all()
    ym_list = sorted({r[0] for r in yms if r[0]}, reverse=True)

    # leer filtros year/month (ya presente)
    today = date.today()
    year = int(request.args.get("year", today.year))
    month_arg = request.args.get("month", None)
    month = int(month_arg) if month_arg is not None else 13

    # nuevo filtro: socio (nombre)
    socio_name = (request.args.get("socio") or "").strip()
    socios = [n for (_id, n) in db.session.query(Socio.id, Socio.nombre).order_by(Socio.nombre).all()]

    # construir ventas_query según year/month (misma lógica que index/ventas/compras)
    if year == 1313 and month == 13:
        ventas_query = db.session.query(Venta)
        compras_query = db.session.query(Compra)
        ym = "all"
    elif month == 13:
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ym = f"{year}-*"
    elif year == 1313:
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ym = "none"
    else:
        ym = f"{year:04d}-{month:02d}"
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)

    # aplicar filtro por nombre_socio si se pidió
    if socio_name:
        ventas_query = ventas_query.join(Socio, Venta.socio_id == Socio.id).filter(Socio.nombre == socio_name)

    total_ventas_con_iva = float(
        ventas_query.with_entities(
            func.coalesce(func.sum(total_con_iva_expr(Venta)), 0.0)
        ).scalar()
        or 0.0
    )

    # total ventas sin IVA (base)
    total_ventas_sin_iva = float(
        ventas_query.with_entities(
            func.coalesce(func.sum(func.coalesce(Venta.pesos_sin_iva, 0.0)), 0.0)
        ).scalar() or 0.0
    )

    # total compras: mismo tratamiento
    total_compras_con_iva = float(
        compras_query.with_entities(
            func.coalesce(func.sum(total_con_iva_expr(Compra)), 0.0)
        ).scalar()
        or 0.0
    )

    # total compras sin IVA (base)
    total_compras_sin_iva = float(
        compras_query.with_entities(
            func.coalesce(func.sum(func.coalesce(Compra.pesos_sin_iva, 0.0)), 0.0)
        ).scalar() or 0.0
    )

    # saldos
    saldo_con_iva = total_ventas_con_iva - total_compras_con_iva
    saldo_sin_iva = total_ventas_sin_iva - total_compras_sin_iva

    # IVA explicit (opcional)
    total_ventas_iva_amt = total_ventas_con_iva - total_ventas_sin_iva
    total_compras_iva_amt = total_compras_con_iva - total_compras_sin_iva

    # Saldo del IVA (ventas IVA - compras IVA)
    total_iva_saldo = total_ventas_iva_amt - total_compras_iva_amt

    # debug
    try:
        app.logger.debug(
            "Resumen Socio - ym=%s ventas_total=%s compras_total=%s ventas_sin_iva=%s compras_sin_iva=%s",
            ym,
            total_ventas_con_iva,
            total_compras_con_iva,
            total_ventas_sin_iva,
            total_compras_sin_iva,
        )
    except Exception:
        pass

    filas, p_emp, p_ven, p_soc = build_resumen_socio(ym)
    return render_template(
        "resumen_socio.html",
        filas=filas,
        ym=ym,
        ym_list=ym_list,
        p_emp=p_emp,
        p_ven=p_ven,
        p_soc=p_soc,
        year=year,
        month=month,
        current_year=today.year,
        total_ventas_con_iva=total_ventas_con_iva,
        total_compras_con_iva=total_compras_con_iva,
        saldo_con_iva=saldo_con_iva,
        total_ventas_sin_iva=total_ventas_sin_iva,
        total_compras_sin_iva=total_compras_sin_iva,
        saldo_sin_iva=saldo_sin_iva,
        total_ventas_iva_amt=total_ventas_iva_amt,
        total_compras_iva_amt=total_compras_iva_amt,
        total_iva_saldo=total_iva_saldo,
    )


@app.route("/resumen-socio/export", endpoint="resumen_socio_export")
def resumen_socio_export():
    """
    Export versión que acepta year/month (preferible) o legacy ym param.
    """
    # construir lista de ym disponibles (legacy)
    yms = db.session.query(Compra.ym).distinct().union(db.session.query(Venta.ym)).all()
    ym_list = sorted({r[0] for r in yms if r[0]}, reverse=True)

    # priorizar year/month si presentes
    today = date.today()
    year_arg = request.args.get("year")
    month_arg = request.args.get("month")
    fmt = request.args.get("format", "csv").lower()

    if year_arg is not None or month_arg is not None:
        year = int(year_arg) if year_arg is not None else today.year
        month = int(month_arg) if month_arg is not None else 13
        if year == 1313 and month == 13:
            ym = "all"
        elif month == 13:
            ym = f"{year}-*"
        elif year == 1313:
            ym = "none"
        else:
            ym = f"{year:04d}-{month:02d}"
    else:
        ym = request.args.get("ym")
        if not ym or ym not in ym_list:
            ym = ym_list[0] if ym_list else None

    if not ym:
        return Response("No hay datos para exportar", status=400)

    filas, p_emp, p_ven, p_soc = build_resumen_socio(ym)

    if fmt == "xlsx":
        if pd is None:
            return Response("Pandas no instalado", status=500)
        df = pd.DataFrame(filas)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=f"Resumen_{ym}")
        bio.seek(0)
        return send_file(
            bio,
            as_attachment=True,
            download_name=f"resumen_socio_{ym}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        sio = io.StringIO()
        writer = csv.DictWriter(
            sio,
            fieldnames=(
                list(filas[0].keys())
                if filas
                else [
                    "YM",
                    "nombre_socio",
                    "Ganancia_neta",
                    "Margen_Empresa",
                    "Margen_Vendedor",
                    "Margen_Socio",
                    "Margen_Otros_Socios",
                    "Total_Margenes",
                    "Total_Caja",
                    "Resto",
                ]
            ),
        )
        writer.writeheader()
        for row in filas:
            writer.writerow(row)
        return Response(
            sio.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename=resumen_socio_{ym}.csv"},
        )


# ------------------- Importación -------------------


def do_import_excel_from_path(path: str):
    """
    Procesa un archivo Excel (ruta local) y lo importa a la base de datos.

    Qué hace:
    - Lee pestañas esperadas: Parametros, Socios, FactCompras, FactVentas.
    - Actualiza/crea parámetros y socios.
    - Valida y convierte filas de compras/ventas, crea objetos Compra/Venta.
    - Maneja rechazos (los guarda en un CSV en uploads/ y devuelve path).
    - Borra previamente los YMs detectados para evitar duplicados (limpieza por periodo).
    - Ajusta márgenes por defecto en Socio si están vacíos.

    Parámetros:
    - path: ruta al archivo XLSX descargado/subido.

    Devuelve:
    - dict con keys: deleted_c, deleted_v, rechazos, rechazos_path.

    Efectos secundarios:
    - Inserta/borra filas en la BD (db.session).
    - Crea archivos en UPLOAD_FOLDER cuando hay rechazos.

    Quién la consume:
    - import_xls route y import_gsheet (descarga y reusa esta función).
    """
    rechazos = []
    socio_oblig = bool(int(get_param("nombre_socio_obligatorio", 1)))
    # Parametros
    try:
        df_par = pd.read_excel(path, sheet_name="Parametros")
        if {"Parametro", "Valor"}.issubset(df_par.columns):
            for _, r in df_par.iterrows():
                clave = str(r.get("Parametro")).strip()
                if not clave:
                    continue
                try:
                    valor = float(r.get("Valor"))
                except Exception:
                    continue
                p = db.session.get(Parametro, clave)
                if p is None:
                    db.session.add(Parametro(clave=clave, valor=valor))
                else:
                    p.valor = valor
            db.session.commit()
    except Exception:
        pass
    # Socios
    try:
        df_soc = pd.read_excel(path, sheet_name="Socios")
        if {"nombre_socio", "tipo_socio"}.issubset(df_soc.columns):
            for _, r in df_soc.iterrows():
                nombre = str(r["nombre_socio"]).strip()
                if not nombre:
                    continue
                tipo = (
                    str(r["tipo_socio"]).strip()
                    if pd.notna(r.get("tipo_socio"))
                    else "Socio"
                )
                s = db.session.query(Socio).filter_by(nombre=nombre).first()
                if not s:
                    db.session.add(Socio(nombre=nombre, tipo=tipo))
                else:
                    s.tipo = tipo
            db.session.commit()
    except Exception:
        pass

    def get_socio_id(nom):
        if nom is None:
            return None
        s = db.session.query(Socio).filter_by(nombre=str(nom).strip()).first()
        return s.id if s else None

    # Detectar YMs a limpiar
    yms_c, yms_v = set(), set()
    try:
        tmp = pd.read_excel(path, sheet_name="FactCompras")
        for _, r in tmp.iterrows():
            f = r.get("FECHA")
            if pd.isna(f):
                continue
            f = parse_date(f) if isinstance(f, str) else pd.to_datetime(f).date()
            yms_c.add(ym_from_date(f))
    except Exception:
        pass
    try:
        tmp = pd.read_excel(path, sheet_name="FactVentas")
        for _, r in tmp.iterrows():
            f = r.get("FECHA")
            if pd.isna(f):
                continue
            f = parse_date(f) if isinstance(f, str) else pd.to_datetime(f).date()
            yms_v.add(ym_from_date(f))
    except Exception:
        pass
    deleted_c = (
        db.session.query(Compra)
        .filter(Compra.ym.in_(list(yms_c)))
        .delete(synchronize_session=False)
        if yms_c
        else 0
    )
    deleted_v = (
        db.session.query(Venta)
        .filter(Venta.ym.in_(list(yms_v)))
        .delete(synchronize_session=False)
        if yms_v
        else 0
    )
    if any([deleted_c, deleted_v]):
        db.session.commit()

    # Helpers parsing
    def _to_bool_si_no(val):
        s = str(val).strip().lower()
        return s in {"si", "sí", "s", "yes", "y", "true", "1"}

    def _to_pct(val, default=None):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        s = str(val).strip().replace(",", ".")
        try:
            if s.endswith("%"):
                p = float(s[:-1]) / 100.0
            else:
                p = float(s)
                if p > 1.0:
                    p = p / 100.0
            return min(max(p, 0.0), 1.0)
        except Exception:
            return default

    # Import Compras
    df_c = pd.read_excel(path, sheet_name="FactCompras")
    for _, r in df_c.iterrows():
        try:
            fecha = r.get("FECHA")
            if pd.isna(fecha):
                continue
            fecha = (
                parse_date(fecha)
                if isinstance(fecha, str)
                else pd.to_datetime(fecha).date()
            )
            ym = ym_from_date(fecha)
            socio_id = get_socio_id(r.get("nombre_socio"))
            if socio_oblig and not socio_id:
                rechazos.append(
                    {
                        "sheet": "FactCompras",
                        "motivo": "nombre_socio inválido/ausente",
                        "NRO_FACTURA": r.get("NRO_FACTURA"),
                        "FECHA": str(fecha),
                        "PROVEEDOR": r.get("PROVEEDOR"),
                    }
                )
                continue
            personal = (
                _to_bool_si_no(r.get("personal"))
                if ("personal" in df_c.columns)
                else False
            )
            p_norm = get_param("iva_deducible_normal_pct", 1.0)
            p_pers_def = get_param("iva_deducible_personal_default_pct", 0.5)
            ded_pct = _to_pct(r.get("iva_deducible_pct"), None)
            if ded_pct is None:
                ded_pct = p_pers_def if personal else p_norm
            ded_pct = min(max(float(ded_pct), 0.0), 1.0)
            cobj = Compra(
                fecha=fecha,
                ym=ym,
                proveedor=str(r.get("PROVEEDOR", "")),
                socio_id=socio_id,
                pesos_sin_iva=float(r.get("PESOS_SIN_IVA") or 0),
                iva_21=float(r.get("IVA_21") or 0),
                iva_105=float(r.get("IVA_105") or 0),
                total_con_iva=float(r.get("TOTAL_CON_IVA") or 0),
                tipo=str(r.get("TIPO") or "").upper(),
                nro_factura=str(r.get("NRO_FACTURA") or ""),
                cuit=str(r.get("CUIT") or ""),
                origen=str(r.get("ORIGEN") or ""),
                estado=str(r.get("ESTADO") or "PAGADO"),
                descripcion=str(r.get("DETALLE") or ""),
                personal=personal,
                iva_deducible_pct=ded_pct,
                transaccion_id=str(r.get("transaccion_id") or "").strip()
            )
            db.session.add(cobj)
        except Exception as e:
            rechazos.append({"sheet": "FactCompras", "motivo": str(e)})
    db.session.commit()
    # Import Ventas
    df_v = pd.read_excel(path, sheet_name="FactVentas")
    for _, r in df_v.iterrows():
        try:
            fecha = r.get("FECHA")
            if pd.isna(fecha):
                continue
            fecha = (
                parse_date(fecha)
                if isinstance(fecha, str)
                else pd.to_datetime(fecha).date()
            )
            ym = ym_from_date(fecha)
            socio_id = get_socio_id(r.get("nombre_socio"))
            if socio_oblig and not socio_id:
                rechazos.append(
                    {
                        "sheet": "FactVentas",
                        "motivo": "nombre_socio inválido/ausente",
                        "NRO_FACTURA": r.get("NRO_FACTURA"),
                        "FECHA": str(fecha),
                        "CLIENTE": r.get("CLIENTE"),
                    }
                )
                continue
            vobj = Venta(
                fecha=fecha,
                ym=ym,
                cliente=str(r.get("CLIENTE", "")),
                socio_id=socio_id,
                pesos_sin_iva=float(r.get("PESOS_SIN_IVA") or 0),
                iva_21=float(r.get("IVA_21") or 0),
                iva_105=float(r.get("IVA_105") or 0),
                total_con_iva=float(r.get("TOTAL_CON_IVA") or 0),
                nro_factura=str(r.get("NRO_FACTURA") or ""),
                cuit_venta=str(r.get("CUIT_VENTA") or ""),
                destino=str(r.get("DESTINO") or ""),
                estado=str(r.get("ESTADO") or "PAGADO"),
                descripcion=str(r.get("DETALLE") or ""),
                tipo=str(r.get("TIPO") or "").upper(),
                transaccion_id=str(r.get("transaccion_id") or "").strip()
            )
            db.session.add(vobj)
        except Exception as e:
            rechazos.append({"sheet": "FactVentas", "motivo": str(e)})
    db.session.commit()
    # Margenes default
    p_emp = _read_param_any(["margen_Empresa"], 0.53)
    p_soc = _read_param_any(["margen_Socio"], 0.09)
    changed = False
    for s in db.session.query(Socio).order_by(Socio.nombre).all():
        if s.margen_porcentaje is None:
            s.margen_porcentaje = p_emp if s.tipo == "Empresa" else p_soc
            changed = True
    if changed:
        db.session.commit()
    # Rechazos file
    rej_file = None
    if rechazos:
        fname = f"rechazos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        fpath = os.path.join(UPLOAD_FOLDER, fname)
        with open(fpath, "w", newline="", encoding="utf-8") as fh:
            fieldnames = sorted({k for r in rechazos for k in r.keys()})
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rechazos)
        rej_file = fpath
    return {
        "deleted_c": deleted_c,
        "deleted_v": deleted_v,
        "rechazos": len(rechazos),
        "rechazos_path": rej_file,
    }


@app.route("/import/xls", methods=["GET", "POST"])
def import_xls():
    """
    Ruta para subir e importar un XLSX via formulario web.

    Qué hace:
    - Si POST: guarda archivo en uploads/, llama a do_import_excel_from_path y muestra mensajes (flash).
    - Si GET: renderiza plantilla con formulario de subida.

    Requiere:
    - pandas + openpyxl instalados para procesar XLSX.

    Quién la consume:
    - Usuario final (admin) que sube el archivo Excel con FactCompras / FactVentas.
    """
    if request.method == "POST":
        if pd is None:
            flash("Pandas no instalado. Ejecutá: pip install pandas openpyxl", "danger")
            return redirect(url_for("import_xls"))
        file = request.files.get("file")
        if not file:
            flash("Subí un archivo .xlsx/.xlsm", "warning")
            return redirect(url_for("import_xls"))
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_XL:
            flash("Formato no soportado", "warning")
            return redirect(url_for("import_xls"))
        filename = secure_filename(file.filename)
        path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(path)
        try:
            res = do_import_excel_from_path(path)
            if res["deleted_c"] or res["deleted_v"]:
                flash(
                    f"Limpieza previa: Compras {res['deleted_c']}, Ventas {res['deleted_v']}",
                    "info",
                )
            if res["rechazos"]:
                flash(
                    f"Importación completa con {res['rechazos']} filas rechazadas.",
                    "warning",
                )
                if res["rechazos_path"]:
                    flash(
                        f"Descargá el detalle: /uploads/{os.path.basename(res['rechazos_path'])}",
                        "info",
                    )
            else:
                flash("Importación desde Excel completa", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"Error importando Excel: {e}", "danger")
        return redirect(url_for("import_xls"))
    return render_template(
        "import_xls.html", default_gsheet_id=app.config["DEFAULT_GSHEET_ID"]
    )


# Normalizar URL/ID


def _normalize_gsheet_export_url(url_or_id: str) -> str:
    """
    Normaliza una URL o ID de Google Sheets para obtener el enlace de exportación XLSX.

    Qué hace:
    - Si la entrada es una URL con docs.google.com, extrae el spreadsheetId.
    - Si es sólo un ID, lo usa directamente.
    - Devuelve una URL '.../export?format=xlsx'.

    Parámetros:
    - url_or_id: URL completa o spreadsheetId.

    Devuelve:
    - str: URL de exportación XLSX válida.

    Quién la consume:
    - import_gsheet que descarga el XLSX desde Google Sheets.
    """
    u = (url_or_id or "").strip()
    if not u:
        raise ValueError("URL/ID de Google Sheet vacío")
    if "docs.google.com" in u:
        if "/export?" in u:
            return u
        try:
            sid = u.split("/d/")[1].split("/")[0]
        except Exception:
            raise ValueError("No pude detectar el spreadsheetId en la URL")
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"
    else:
        sid = u
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=xlsx"


@app.route("/import/gsheet", methods=["POST"])
def import_gsheet():
    """
    Descarga un Google Sheet (XLSX) y lo importa reusando do_import_excel_from_path.

    Qué hace:
    - Normaliza la URL/ID, descarga el XLSX con requests, lo guarda en uploads/ y llama a do_import_excel_from_path.
    - Maneja y muestra errores vía flash.

    Quién la consume:
    - Formulario de importación que permite pasar una URL o ID de Google Sheets.
    """
    if pd is None:
        flash("Pandas no instalado. Ejecutá: pip install pandas openpyxl", "danger")
        return redirect(url_for("import_xls"))
    if requests is None:
        flash("Falta requests. Ejecutá: pip install requests", "danger")
        return redirect(url_for("import_xls"))
    sid = (request.form.get("sid") or "").strip()
    url_in = (request.form.get("url") or "").strip()
    source = sid or url_in or app.config["DEFAULT_GSHEET_ID"]
    try:
        export_url = _normalize_gsheet_export_url(source)
    except Exception as e:
        flash(str(e), "danger")
        return redirect(url_for("import_xls"))
    ts = time.strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(app.config["UPLOAD_FOLDER"], f"gsheet_{ts}.xlsx")
    try:
        r = requests.get(export_url, timeout=60)
        r.raise_for_status()
        with open(dest, "wb") as f:
            f.write(r.content)
    except Exception as e:
        flash(f"No pude descargar el XLSX desde Google Sheets: {e}", "danger")
        return redirect(url_for("import_xls"))
    try:
        res = do_import_excel_from_path(dest)
        if res["deleted_c"] or res["deleted_v"]:
            flash(
                f"Limpieza previa: Compras {res['deleted_c']}, Ventas {res['deleted_v']}",
                "info",
            )
        if res["rechazos"]:
            flash(
                f"Importación (Google Sheets) completa con {res['rechazos']} filas rechazadas.",
                "warning",
            )
            if res["rechazos_path"]:
                flash(
                    f"Descargá el detalle: /uploads/{os.path.basename(res['rechazos_path'])}",
                    "info",
                )
        else:
            flash("Importación desde Google Sheets completa", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error importando el XLSX descargado: {e}", "danger")
    return redirect(url_for("import_xls"))


@app.route("/uploads/<path:filename>")
def download_upload(filename):
    """
    Sirve archivos estáticos guardados en UPLOAD_FOLDER (descarga).

    Quién la consume:
    - Enlaces que muestran paths de archivos generados (rechazos, export files).
    """
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename, as_attachment=True)


def backup_db(prefix: str = "backup") -> str:
    """
    Crea una copia del archivo SQLite DB en BACKUPS_FOLDER con prefijo y timestamp.

    Parámetros:
    - prefix: etiqueta para el backup (ej: 'clean').

    Devuelve:
    - ruta al fichero de backup o cadena vacía si falla.

    Quién la consume:
    - import_clean y otros lugares donde se necesite snapshot previo a cambios destructivos.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUPS_FOLDER, f"{prefix}_{ts}.db")
    try:
        shutil.copy2(DB_PATH, dst)
        return dst
    except Exception:
        return ""


@app.route("/socios", methods=["GET", "POST"])
def socios_view():
    """
    CRUD simple para socios (creación y listado).

    Qué hace:
    - Si POST: valida y crea un nuevo Socio.
    - Si GET: lista socios ordenados y garantiza que margen_porcentaje tenga valor por defecto.

    Quién la consume:
    - Plantilla 'socios_list.html' y la UI de administración.
    """
    p_emp = _read_param_any(["margen_Empresa"], 0.53)
    p_soc = _read_param_any(["margen_Socio"], 0.09)
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        tipo = request.form.get("tipo", "Socio")
        margen = request.form.get("margen_porcentaje")
        margen = float(margen) if margen else (p_emp if tipo == "Empresa" else p_soc)
        if not nombre:
            flash("El nombre es obligatorio", "danger")
        else:
            if db.session.query(Socio).filter_by(nombre=nombre).first():
                flash("Ya existe un socio con ese nombre", "warning")
            else:
                s = Socio(nombre=nombre, tipo=tipo, margen_porcentaje=margen)
                db.session.add(s)
                db.session.commit()
                flash("Socio creado", "success")
        return redirect(url_for("socios_view"))
    changed = False
    for s in db.session.query(Socio).all():
        if s.margen_porcentaje is None:
            s.margen_porcentaje = p_emp if s.tipo == "Empresa" else p_soc
            changed = True
    if changed:
        db.session.commit()
    socios = db.session.query(Socio).order_by(Socio.nombre).all()
    return render_template("socios_list.html", socios=socios, p_emp=p_emp, p_soc=p_soc)


@app.route("/compras")
def compras_list():
    """
    Lista de compras con filtros year/month y filtro por socio (nombre).

    Qué hace:
    - Construye compras_query según filtros year/month.
    - Aplica filtro por nombre de socio (si viene).
    - Soporta export (export=csv|xlsx) devolviendo filas con total_con_iva calculado (fallback).
    - Renderiza plantilla 'compras_list.html' con variables: compras, year, month, current_year, socios, selected_socio.

    Parámetros:
    - year, month, socio, export

    Quién la consume:
    - Usuario en la UI (listado, export).
    """
    today = date.today()
    year = int(request.args.get("year", today.year))

    month_arg = request.args.get("month", None)
    month = int(month_arg) if month_arg is not None else 13

    # nuevo filtro: socio (nombre)
    socio_name = (request.args.get("socio") or "").strip()
    estado_filtro = (request.args.get("estado") or "").strip()
    # lista de socios para el select en la plantilla
    socios = [n for (_id, n) in db.session.query(Socio.id, Socio.nombre).order_by(Socio.nombre).all()]

    # Construir compras_query según year/month (misma lógica que index/ventas/compras)
    if year == 1313 and month == 13:
        compras_query = db.session.query(Compra)
        ym = "all"
    elif month == 13:
        compras_query = db.session.query(Compra).filter(Compra.ym.like(f"{year}-%"))
        ym = f"{year}-*"
    elif year == 1313:
        compras_query = db.session.query(Compra).filter(Compra.id == -1)
        ym = "none"
    else:
        ym = f"{year:04d}-{month:02d}"
        compras_query = db.session.query(Compra).filter(Compra.ym == ym)

    # aplicar filtro por nombre_socio si se pidió
    if socio_name:
        # join con Socio y filtrar por nombre (exact match). Cambia a .like(...) si querés búsqueda parcial.
        compras_query = compras_query.join(Socio, Compra.socio_id == Socio.id).filter(Socio.nombre == socio_name)

    # aplicar filtro por estado si se pidió
    if estado_filtro:
        compras_query = compras_query.filter(Compra.estado == estado_filtro)

    # Calcular el total con IVA para el período filtrado
    total_compras_con_iva = float(
        compras_query.with_entities(
            func.coalesce(func.sum(total_con_iva_expr(Compra)), 0.0)
        ).scalar()
        or 0.0
    )

    export_fmt = (request.args.get("export") or "").lower()

    if export_fmt:
        # preparar filas para export (lista de dicts)
        rows = []
        compras_iter = compras_query.order_by(Compra.fecha.desc()).all()
        socios_map = {sid: nom for sid, nom in db.session.query(Socio.id, Socio.nombre).all()}
        for c in compras_iter:
            rows.append(
                {
                    "fecha": c.fecha.strftime("%Y-%m-%d") if c.fecha else "",
                    "proveedor": c.proveedor or "",
                    "socio": socios_map.get(c.socio_id, ""),
                    "pesos_sin_iva": round(float(c.pesos_sin_iva or 0.0), 2),
                    "iva_21": round(float(c.iva_21 or 0.0), 2),
                    "iva_105": round(float(c.iva_105 or 0.0), 2),
                    "total_con_iva": round(float(c.total_con_iva or ((c.pesos_sin_iva or 0.0) + (c.iva_21 or 0.0) + (c.iva_105 or 0.0))), 2),
                    "estado": c.estado or "",
                    "descripcion": c.descripcion or "",
                    "nro_factura": c.nro_factura or "",
                }
            )

        if export_fmt == "xlsx":
            if pd is None:
                return Response("Pandas no instalado", status=500)
            df = pd.DataFrame(rows)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=f"Compras_{ym}")
            bio.seek(0)
            return send_file(
                bio,
                as_attachment=True,
                download_name=f"compras_{ym}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            sio = io.StringIO()
            if rows:
                writer = csv.DictWriter(sio, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            return Response(
                sio.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=compras_{ym}.csv"},
            )

    # vista HTML normal: pasar year/month al template para que los selects funcionen
    compras = compras_query.order_by(Compra.fecha.desc()).limit(300).all()
    # Añadir color_index para el coloreado de transacciones
    for c in compras:
        if c.transaccion_id:
            c.color_index = color_index(c.transaccion_id, 8)
        else:
            c.color_index = None
    return render_template(
        "compras_list.html",
        compras=compras,
        total_compras_con_iva=total_compras_con_iva,
        year=year,
        month=month,
        current_year=today.year,
        socios=socios,
        selected_socio=socio_name,
        selected_estado=estado_filtro,
    )


@app.route("/ventas/export_xlsx")
def export_ventas_xls():
    """
    Endpoint auxiliar (opción B): mantiene el nombre antiguo `export_ventas_xls`.
    Redirige a la vista `ventas_list` con el parámetro export='xlsx' para
    reutilizar la lógica centralizada de export en ventas_list.
    """
    year = request.args.get("year")
    month = request.args.get("month")
    return redirect(url_for("ventas_list", year=year, month=month, export="xlsx"))


@app.route("/ventas")
def ventas_list():
    """
    Listado de ventas con soporte de filtro year/month (month=1-12, 13=Todos)
    y soporte de export (export='csv' | 'xlsx').

    Qué hace:
    - Construye ventas_query según filtros year/month.
    - Aplica filtro por nombre de socio (si viene).
    - Soporta export (export=csv|xlsx) devolviendo filas con total_con_iva calculado (fallback).
    - Renderiza plantilla 'ventas_list.html' con variables: ventas, year, month, current_year, socios, selected_socio.

    Parámetros:
    - year, month, socio, export

    Quién la consume:
    - Usuario en la UI (listado, export).
    """
    today = date.today()
    year = int(request.args.get("year", today.year))

    month_arg = request.args.get("month", None)
    month = int(month_arg) if month_arg is not None else 13

    # nuevo filtro: socio (nombre)
    socio_name = (request.args.get("socio") or "").strip()
    estado_filtro = (request.args.get("estado") or "").strip()
    # lista de socios para el select en la plantilla
    socios = [n for (_id, n) in db.session.query(Socio.id, Socio.nombre).order_by(Socio.nombre).all()]

    # Construir ventas_query según year/month (misma lógica que index)
    if year == 1313 and month == 13:
        ventas_query = db.session.query(Venta)
        ym = "all"
    elif month == 13:
        ventas_query = db.session.query(Venta).filter(Venta.ym.like(f"{year}-%"))
        ym = f"{year}-*"
    elif year == 1313:
        ventas_query = db.session.query(Venta).filter(Venta.id == -1)
        ym = "none"
    else:
        ym = f"{year:04d}-{month:02d}"
        ventas_query = db.session.query(Venta).filter(Venta.ym == ym)

    # aplicar filtro por nombre_socio si se pidió
    if socio_name:
        ventas_query = ventas_query.join(Socio, Venta.socio_id == Socio.id).filter(Socio.nombre == socio_name)

    # aplicar filtro por estado si se pidió
    if estado_filtro:
        ventas_query = ventas_query.filter(Venta.estado == estado_filtro)

    # Calcular el total con IVA para el período filtrado
    total_ventas_con_iva = float(
        ventas_query.with_entities(
            func.coalesce(func.sum(total_con_iva_expr(Venta)), 0.0)
        ).scalar()
        or 0.0
    )

    export_fmt = (request.args.get("export") or "").lower()

    if export_fmt:
        # preparar filas para export (lista de dicts)
        rows = []
        ventas_iter = ventas_query.order_by(Venta.fecha.desc()).all()
        socios_map = {sid: nom for sid, nom in db.session.query(Socio.id, Socio.nombre).all()}
        for v in ventas_iter:
            rows.append(
                {
                    "fecha": v.fecha.strftime("%Y-%m-%d") if v.fecha else "",
                    "cliente": v.cliente or "",
                    "socio": socios_map.get(v.socio_id, ""),
                    "pesos_sin_iva": round(float(v.pesos_sin_iva or 0.0), 2),
                    "iva_21": round(float(v.iva_21 or 0.0), 2),
                    "iva_105": round(float(v.iva_105 or 0.0), 2),
                    "total_con_iva": round(float(v.total_con_iva or ((v.pesos_sin_iva or 0.0) + (v.iva_21 or 0.0) + (v.iva_105 or 0.0))), 2),
                    "estado": v.estado or "",
                    "descripcion": v.descripcion or "",
                    "nro_factura": v.nro_factura or "",
                }
            )

        if export_fmt == "xlsx":
            if pd is None:
                return Response("Pandas no instalado", status=500)
            df = pd.DataFrame(rows)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=f"Ventas_{ym}")
            bio.seek(0)
            return send_file(
                bio,
                as_attachment=True,
                download_name=f"ventas_{ym}.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            # csv
            sio = io.StringIO()
            if rows:
                writer = csv.DictWriter(sio, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            return Response(
                sio.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename=ventas_{ym}.csv"},
            )

    # vista HTML normal: pasar year/month al template para que los selects funcionen
    ventas = ventas_query.order_by(Venta.fecha.desc()).limit(300).all()
    # Añadir color_index para el coloreado de transacciones
    for v in ventas:
        if v.transaccion_id:
            v.color_index = color_index(v.transaccion_id, 8)
        else:
            v.color_index = None
    return render_template(
        "ventas_list.html",
        ventas=ventas,
        total_ventas_con_iva=total_ventas_con_iva,
        year=year,
        month=month,
        current_year=today.year,
        socios=socios,
        selected_socio=socio_name,
        selected_estado=estado_filtro,
    )


# ------------------- SUMAS -------------------
from sqlalchemy import func

def total_con_iva_expr(Model):
    """
    Crea una expresión SQLAlchemy que por fila devuelve:
      NULLIF(Model.total_con_iva, 0) OR (pesos_sin_iva + iva_21 + iva_105)

    Uso:
    - Utilizar en SELECTs y en agregaciones GROUP BY para sumar el total correcto
      incluso cuando la columna total_con_iva se guardó como 0 (fallback calculado).

    Parámetros:
    - Model: la clase SQLAlchemy (Compra o Venta).

    Devuelve:
    - SQLAlchemy ColumnElement etiquetado como 'TOTAL_CON_IVA' (útil en .with_entities).

    Quién la consume:
    - Vistas que agrupan/suman total_con_iva (totales_arca, resumen_socio, etc.)
    """
    return func.coalesce(
        func.nullif(getattr(Model, "total_con_iva"), 0),
        (
            func.coalesce(getattr(Model, "pesos_sin_iva"), 0.0)
            + func.coalesce(getattr(Model, "iva_21"), 0.0)
            + func.coalesce(getattr(Model, "iva_105"), 0.0)
        ),
    ).label("TOTAL_CON_IVA")


# ------------------- MAIN -------------------
if __name__ == '__main__':
    # Ejecutar la app en desarrollo, accesible desde host (útil en contenedor)
    app.run(host='0.0.0.0', port=5000, debug=True)
