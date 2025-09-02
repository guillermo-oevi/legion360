# -*- coding: utf-8 -*-
"""
Servicio ARCA: consolida Resumen y Totales considerando **solo** comprobantes
TIPO A o B, excluyendo cualquier otro tipo (p.ej. 'N').

Integración esperada:
- Modelos SQLAlchemy: Compra y Venta, con campos:
  fecha (date/datetime), tipo (str), nro_factura, cuit, proveedor/cliente,
  pesos_sin_iva (Numeric), iva_21 (Numeric), iva_105 (Numeric), total_con_iva (Numeric),
  estado, origen/destino, nombre_socio.
- Extensión: db (SQLAlchemy) en app.extensions

Si tu proyecto usa otro path de import, ajustá las importaciones debajo.
"""
from __future__ import annotations
from decimal import Decimal
from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any

from sqlalchemy import func

try:
    # Ajustá estos imports si tu estructura difiere
    from app.extensions import db
    from app.models.compra import Compra
    from app.models.venta import Venta
except Exception as e:  # pragma: no cover - se ajusta en proyecto real
    db = None
    Compra = None
    Venta = None

ALLOWED_ARCA_TYPES = ("A", "B")


def normalize_tipo(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip().upper()
    return v if v else None


@dataclass
class ArcaRow:
    tipo_operacion: str
    fecha: Any
    tipo_comprobante: str
    nro_factura: Optional[str]
    cuit: Optional[str]
    denominacion: Optional[str]
    pesos_sin_iva: Decimal
    iva_21: Decimal
    iva_105: Decimal
    total_con_iva: Decimal
    estado: Optional[str]
    origen_destino: Optional[str]
    nombre_socio: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'tipo_operacion': self.tipo_operacion,
            'fecha': self.fecha.isoformat() if hasattr(self.fecha, 'isoformat') else self.fecha,
            'tipo_comprobante': self.tipo_comprobante,
            'nro_factura': self.nro_factura,
            'cuit': self.cuit,
            'Denominación': self.denominacion,
            'PESOS_SIN_IVA': float(self.pesos_sin_iva),
            'IVA_21': float(self.iva_21),
            'IVA_105': float(self.iva_105),
            'TOTAL_CON_IVA': float(self.total_con_iva),
            'estado': self.estado,
            'origen_destino': self.origen_destino,
            'nombre_socio': self.nombre_socio,
        }


def _session():
    assert db is not None, "No se pudo importar 'db' desde app.extensions. Ajustá el import."
    return db.session


def _query_resumen_compra(fecha_desde=None, fecha_hasta=None):
    q = _session().query(
        func.literal("COMPRA").label("tipo_operacion"),
        Compra.fecha.label("fecha"),
        Compra.tipo.label("tipo_comprobante"),
        Compra.nro_factura.label("nro_factura"),
        Compra.cuit.label("cuit"),
        Compra.proveedor.label("denominacion"),
        Compra.pesos_sin_iva.label("pesos_sin_iva"),
        Compra.iva_21.label("iva_21"),
        Compra.iva_105.label("iva_105"),
        Compra.total_con_iva.label("total_con_iva"),
        Compra.estado.label("estado"),
        Compra.origen.label("origen_destino"),
        Compra.nombre_socio.label("nombre_socio"),
    ).filter(Compra.tipo.in_(ALLOWED_ARCA_TYPES))
    if fecha_desde:
        q = q.filter(Compra.fecha >= fecha_desde)
    if fecha_hasta:
        q = q.filter(Compra.fecha <= fecha_hasta)
    return q


def _query_resumen_venta(fecha_desde=None, fecha_hasta=None):
    q = _session().query(
        func.literal("VENTA").label("tipo_operacion"),
        Venta.fecha.label("fecha"),
        Venta.tipo.label("tipo_comprobante"),
        Venta.nro_factura.label("nro_factura"),
        Venta.cuit.label("cuit"),
        Venta.cliente.label("denominacion"),
        Venta.pesos_sin_iva.label("pesos_sin_iva"),
        Venta.iva_21.label("iva_21"),
        Venta.iva_105.label("iva_105"),
        Venta.total_con_iva.label("total_con_iva"),
        Venta.estado.label("estado"),
        Venta.destino.label("origen_destino"),
        Venta.nombre_socio.label("nombre_socio"),
    ).filter(Venta.tipo.in_(ALLOWED_ARCA_TYPES))
    if fecha_desde:
        q = q.filter(Venta.fecha >= fecha_desde)
    if fecha_hasta:
        q = q.filter(Venta.fecha <= fecha_hasta)
    return q


def get_resumen_arca(fecha_desde=None, fecha_hasta=None) -> List[ArcaRow]:
    """Devuelve filas ARCA (compras+ventas) filtradas a tipos A/B.
    """
    rows = (
        _query_resumen_compra(fecha_desde, fecha_hasta)
        .union_all(_query_resumen_venta(fecha_desde, fecha_hasta))
        .order_by("fecha")
        .all()
    )
    out: List[ArcaRow] = []
    for r in rows:
        m = r._mapping
        out.append(
            ArcaRow(
                tipo_operacion=m['tipo_operacion'],
                fecha=m['fecha'],
                tipo_comprobante=m['tipo_comprobante'],
                nro_factura=m['nro_factura'],
                cuit=m['cuit'],
                denominacion=m['denominacion'],
                pesos_sin_iva=m['pesos_sin_iva'] or 0,
                iva_21=m['iva_21'] or 0,
                iva_105=m['iva_105'] or 0,
                total_con_iva=m['total_con_iva'] or 0,
                estado=m['estado'],
                origen_destino=m['origen_destino'],
                nombre_socio=m['nombre_socio'],
            )
        )
    return out


def _ym(d):
    try:
        return d.strftime('%Y-%m')
    except Exception:
        # Si d ya es str 'YYYY-MM-DD'
        s = str(d)
        return s[:7]


def compute_totales_arca(rows: Iterable[ArcaRow]) -> List[Dict[str, Any]]:
    """Agrupa filas por YM y tipo_operacion, sumando montos y calculando
    Saldo_Tecnico_IVA por YM (se calcula a nivel de reporte final).
    """
    from collections import defaultdict
    agg = defaultdict(lambda: {
        'PESOS_SIN_IVA': Decimal('0'),
        'IVA_21': Decimal('0'),
        'IVA_105': Decimal('0'),
        'TOTAL_CON_IVA': Decimal('0'),
    })
    for r in rows:
        key = (_ym(r.fecha), r.tipo_operacion)
        a = agg[key]
        a['PESOS_SIN_IVA'] += Decimal(r.pesos_sin_iva)
        a['IVA_21']        += Decimal(r.iva_21)
        a['IVA_105']       += Decimal(r.iva_105)
        a['TOTAL_CON_IVA'] += Decimal(r.total_con_iva)

    # Convertimos a lista y calculamos saldo técnico por YM
    by_ym = {}
    out = []
    for (ym, tipo_op), vals in sorted(agg.items()):
        row = {
            'YM': ym,
            'tipo_operacion': tipo_op,
            'PESOS_SIN_IVA': float(vals['PESOS_SIN_IVA']),
            'IVA_21': float(vals['IVA_21']),
            'IVA_105': float(vals['IVA_105']),
            'TOTAL_CON_IVA': float(vals['TOTAL_CON_IVA']),
            'Saldo_Tecnico_IVA': None,  # se completa abajo
        }
        out.append(row)
        s = by_ym.setdefault(ym, {'venta': 0.0, 'compra': 0.0})
        total_iva = float(vals['IVA_21'] + vals['IVA_105'])
        if tipo_op.upper() == 'VENTA':
            s['venta'] += total_iva
        else:
            s['compra'] += total_iva
    # aplicar saldo técnico en cada fila
    for row in out:
        ym = row['YM']
        saldo = by_ym[ym]['venta'] - by_ym[ym]['compra']
        row['Saldo_Tecnico_IVA'] = float(saldo)
    return out


def get_totales_arca(fecha_desde=None, fecha_hasta=None) -> List[Dict[str, Any]]:
    rows = get_resumen_arca(fecha_desde, fecha_hasta)
    return compute_totales_arca(rows)
