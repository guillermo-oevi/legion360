
from flask import request, render_template
from datetime import date
from models import db, Compra, Venta

@app.route("/resumen-caja")
def resumen_caja():
    today = date.today()
    year = int(request.args.get("year", today.year))
    month = int(request.args.get("month", 13))
    caja_filtro = request.args.get("caja", "").strip()

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

    resumen = {}
    cajas = set()

    for c in compras_query.all():
        if not c.origen:
            continue
        cajas.add(c.origen)
        resumen.setdefault(c.origen, []).append({
            "tipo": "COMPRA",
            "fecha": c.fecha,
            "detalle": c.descripcion,
            "monto": -float(c.total_con_iva or 0.0)
        })

    for v in ventas_query.all():
        if not v.destino:
            continue
        cajas.add(v.destino)
        resumen.setdefault(v.destino, []).append({
            "tipo": "VENTA",
            "fecha": v.fecha,
            "detalle": v.descripcion,
            "monto": float(v.total_con_iva or 0.0)
        })

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
        current_year=today.year,
        months={
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
    )
