
import csv, io
import pandas as pd
from flask import Response, send_file, request
from models import db, Compra, Venta
from datetime import date

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
