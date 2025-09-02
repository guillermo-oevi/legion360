# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date
from flask import Blueprint, request, jsonify

from app.services.arca import get_resumen_arca, get_totales_arca

bp = Blueprint('arca', __name__, url_prefix='/arca')


def _parse_date(s):
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        # formatos alternativos: dd/mm/yyyy
        try:
            d,m,y = s.split('/')
            return date(int(y), int(m), int(d))
        except Exception:
            return None


@bp.get('/resumen')
def resumen():
    fdesde = _parse_date(request.args.get('desde'))
    fhasta = _parse_date(request.args.get('hasta'))
    rows = get_resumen_arca(fdesde, fhasta)
    return jsonify([r.to_dict() for r in rows])


@bp.get('/totales')
def totales():
    fdesde = _parse_date(request.args.get('desde'))
    fhasta = _parse_date(request.args.get('hasta'))
    data = get_totales_arca(fdesde, fhasta)
    return jsonify(data)
