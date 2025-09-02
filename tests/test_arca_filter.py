# -*- coding: utf-8 -*-
import datetime as dt
from app.services.arca import ArcaRow, compute_totales_arca, ALLOWED_ARCA_TYPES


def test_compute_totales_arca_filters_values():
    rows = [
        ArcaRow('VENTA', dt.date(2025,7,1),'A','1','20','C1',100,21,0,121,'PAGADO','GALICIA','Guille'),
        ArcaRow('VENTA', dt.date(2025,7,2),'B','2','20','C2',200,0,21,221,'PAGADO','GALICIA','Guille'),
        ArcaRow('COMPRA',dt.date(2025,7,3),'A','3','20','P1',50,10,0,60,'PAGADO','GALICIA','Abel'),
    ]
    tot = compute_totales_arca(rows)
    # Debe haber 2 filas de julio 2025 (una VENTA y una COMPRA)
    ym = { (t['YM'], t['tipo_operacion']) for t in tot }
    assert ('2025-07','VENTA') in ym and ('2025-07','COMPRA') in ym
    # Saldo técnico = IVA venta total - IVA compra total = (21+21) - (10) = 32
    st = [t for t in tot if t['tipo_operacion']=='VENTA'][0]['Saldo_Tecnico_IVA']
    assert abs(st - 32.0) < 1e-6


def test_allowed_types():
    assert set(ALLOWED_ARCA_TYPES) == {"A","B"}
