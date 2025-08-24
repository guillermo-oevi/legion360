
from datetime import date, datetime
from flask import Flask, render_template, request, redirect, url_for, flash, Response, send_file, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.utils import secure_filename
import os, io, csv

try:
    import pandas as pd
except Exception:
    pd = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'app.db')
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + DB_PATH
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'change-me-in-prod')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

ALLOWED_XL = {'.xlsx', '.xlsm', '.xls'}

db = SQLAlchemy(app)

class Socio(db.Model):
    __tablename__ = 'socios'
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), unique=True, nullable=False)
    tipo = db.Column(db.String(20), default='Socio')  # 'Socio' | 'Empresa'
    margen_porcentaje = db.Column(db.Float, nullable=True)

class Parametro(db.Model):
    __tablename__ = 'parametros'
    clave = db.Column(db.String(100), primary_key=True)
    valor = db.Column(db.Float, nullable=False)

class Compra(db.Model):
    __tablename__ = 'compras'
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    ym = db.Column(db.String(7), index=True)
    proveedor = db.Column(db.String(120))
    socio_id = db.Column(db.Integer, db.ForeignKey('socios.id'), nullable=True)
    pesos_sin_iva = db.Column(db.Float, default=0.0)
    iva_21 = db.Column(db.Float, default=0.0)
    iva_105 = db.Column(db.Float, default=0.0)
    total_con_iva = db.Column(db.Float, default=0.0)
    tipo = db.Column(db.String(5))
    nro_factura = db.Column(db.String(50))
    cuit = db.Column(db.String(20))
    origen = db.Column(db.String(50))
    estado = db.Column(db.String(20), default='PAGADO')
    descripcion = db.Column(db.String(255))

class CompraPersonal(db.Model):
    __tablename__ = 'compras_personales'
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    ym = db.Column(db.String(7), index=True)
    proveedor = db.Column(db.String(120))
    socio_id = db.Column(db.Integer, db.ForeignKey('socios.id'), nullable=True)
    iva_21 = db.Column(db.Float, default=0.0)
    iva_105 = db.Column(db.Float, default=0.0)
    tipo = db.Column(db.String(5))
    nro_factura = db.Column(db.String(50))
    cuit = db.Column(db.String(20))
    origen = db.Column(db.String(50))
    estado = db.Column(db.String(20), default='PAGADO')

class Venta(db.Model):
    __tablename__ = 'ventas'
    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False)
    ym = db.Column(db.String(7), index=True)
    cliente = db.Column(db.String(120))
    socio_id = db.Column(db.Integer, db.ForeignKey('socios.id'), nullable=True)
    pesos_sin_iva = db.Column(db.Float, default=0.0)
    iva_21 = db.Column(db.Float, default=0.0)
    iva_105 = db.Column(db.Float, default=0.0)
    total_con_iva = db.Column(db.Float, default=0.0)
    nro_factura = db.Column(db.String(50))
    cuit_venta = db.Column(db.String(20))
    destino = db.Column(db.String(50))
    estado = db.Column(db.String(20), default='PAGADO')
    descripcion = db.Column(db.String(255))

# -------- Helpers --------

def parse_date(dstr: str):
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(dstr.strip(), fmt).date()
        except Exception:
            pass
    raise ValueError(f'Fecha inválida: {dstr}')

def ym_from_date(d: date):
    return f"{d.year:04d}-{d.month:02d}"

@app.template_filter('ars')
def format_ars(value, digits=2):
    try:
        n = float(value or 0)
    except Exception:
        n = 0.0
    s = f"{n:,.{digits}f}"  # 1,234,567.89
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f"${s}"

# convenience param getter

def get_param(clave: str, default: float | None = None) -> float:
    p = db.session.get(Parametro, clave)
    if p is None:
        if default is None:
            raise RuntimeError(f'Parametro {clave} no encontrado y sin default')
        # crear param si no existe
        p = Parametro(clave=clave, valor=default)
        db.session.add(p)
        db.session.commit()
        return default
    return p.valor

with app.app_context():
    db.create_all()
    # crear por si faltan (usar db.session.get en lugar de Query.get)
    if db.session.get(Parametro, 'margen_Empresa') is None:
        db.session.add(Parametro(clave='margen_Empresa', valor=0.53))
    if db.session.get(Parametro, 'margen_Vendedor') is None:
        db.session.add(Parametro(clave='margen_Vendedor', valor=0.20))
    if db.session.get(Parametro, 'margen_Socio') is None:
        db.session.add(Parametro(clave='margen_Socio', valor=0.09))
    if db.session.get(Parametro, 'nombre_socio_obligatorio') is None:
        db.session.add(Parametro(clave='nombre_socio_obligatorio', valor=1.0))
    db.session.commit()

# --------------------- Build resumen socio ---------------------

def build_resumen_socio(ym: str):
    p_emp = get_param('margen_Empresa', 0.53)
    p_ven = get_param('margen_Vendedor', 0.20)
    p_soc = get_param('margen_Socio', 0.09)

    ventas_sub = db.session.query(
        Venta.socio_id.label('socio_id'),
        func.sum(Venta.pesos_sin_iva).label('ventas_sin_iva')
    ).filter(Venta.ym == ym).group_by(Venta.socio_id).subquery()

    compras_sub = db.session.query(
        Compra.socio_id.label('socio_id'),
        func.sum(Compra.pesos_sin_iva).label('compras_sin_iva')
    ).filter(Compra.ym == ym).group_by(Compra.socio_id).subquery()

    q = db.session.query(
        Socio.id, Socio.nombre, Socio.tipo,
        (func.coalesce(ventas_sub.c.ventas_sin_iva, 0.0) - func.coalesce(compras_sub.c.compras_sin_iva, 0.0)).label('ganancia_neta')
    ).outerjoin(ventas_sub, ventas_sub.c.socio_id == Socio.id
    ).outerjoin(compras_sub, compras_sub.c.socio_id == Socio.id)

    socios = []
    for sid, nombre, tipo, gn in q.all():
        socios.append({'id': sid, 'nombre': nombre, 'tipo': tipo, 'gn': float(gn or 0.0)})

    filas = []
    for s in socios:
        gn = s['gn']
        tipo = s['tipo']
        margen_empresa = round(gn * p_emp, 2)
        margen_vendedor = round(gn * p_ven, 2)
        margen_socio = round(gn * p_soc, 2) if tipo == 'Socio' else 0.0
        margen_otros = 0.0
        for o in socios:
            if o['id'] == s['id']:
                continue
            if tipo == 'Socio' and o['tipo'] == 'Socio':
                margen_otros += round(o['gn'] * p_soc, 2)
            elif tipo == 'Empresa' and o['tipo'] == 'Socio':
                margen_otros += round(o['gn'] * p_emp, 2)
        total_margenes = round(margen_vendedor + margen_socio + margen_otros, 2)
        filas.append({
            'YM': ym,
            'nombre_socio': s['nombre'],
            'Ganancia_neta': round(gn, 2),
            'Margen_Empresa': margen_empresa,
            'Margen_Vendedor': margen_vendedor,
            'Margen_Socios': margen_socio,
            'Margen_Otros_Socios': round(margen_otros, 2),
            'Total_Margenes': total_margenes,
        })
    return filas, p_emp, p_ven, p_soc

# --------------------- Dashboard ---------------------
@app.route('/')
def index():
    today = date.today()
    year = int(request.args.get('year', today.year))
    month = int(request.args.get('month', today.month))
    ym = f"{year:04d}-{month:02d}"

    v = db.session.query(
        func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Venta.iva_21 + Venta.iva_105), 0.0)
    ).filter(Venta.ym == ym).first()
    ventas_sin_iva, iva_venta = float(v[0]), float(v[1])

    c = db.session.query(
        func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Compra.iva_21 + Compra.iva_105), 0.0)
    ).filter(Compra.ym == ym).first()
    compras_sin_iva, iva_compra = float(c[0]), float(c[1])

    p = db.session.query(func.coalesce(func.sum(CompraPersonal.iva_21 + CompraPersonal.iva_105), 0.0)).filter(CompraPersonal.ym == ym).scalar()
    iva_personal_total = float(p or 0.0)
    iva_personal_credito_empresa = iva_personal_total * 0.5
    iva_personal_credito_socios = iva_personal_total * 0.5

    iva_compra_creditable = iva_compra + iva_personal_credito_empresa

    margen_sin_iva = ventas_sin_iva - compras_sin_iva
    iva_a_pagar = iva_venta - iva_compra_creditable

    adeudado_compras = db.session.query(func.count(Compra.id)).filter((Compra.ym == ym) & (Compra.estado == 'ADEUDADO')).scalar()
    adeudado_ventas = db.session.query(func.count(Venta.id)).filter((Venta.ym == ym) & (Venta.estado == 'ADEUDADO')).scalar()

    ventas_sub = db.session.query(
        Venta.socio_id.label('socio_id'),
        func.sum(Venta.pesos_sin_iva).label('ventas_sin_iva')
    ).filter(Venta.ym == ym).group_by(Venta.socio_id).subquery()

    compras_sub = db.session.query(
        Compra.socio_id.label('socio_id'),
        func.sum(Compra.pesos_sin_iva).label('compras_sin_iva')
    ).filter(Compra.ym == ym).group_by(Compra.socio_id).subquery()

    q = db.session.query(
        Socio.nombre,
        func.coalesce(ventas_sub.c.ventas_sin_iva, 0.0),
        func.coalesce(compras_sub.c.compras_sin_iva, 0.0)
    ).outerjoin(ventas_sub, ventas_sub.c.socio_id == Socio.id
    ).outerjoin(compras_sub, compras_sub.c.socio_id == Socio.id)

    per_socio = []
    for nombre, v_sin, c_sin in q.all():
        per_socio.append({
            'nombre': nombre,
            'ventas_sin_iva': float(v_sin or 0.0),
            'compras_sin_iva': float(c_sin or 0.0),
            'ganancia_neta': float((v_sin or 0.0) - (c_sin or 0.0))
        })

    return render_template('index.html',
                           year=year, month=month,
                           ventas_tot={'monto_total': ventas_sin_iva + iva_venta, 'iva': iva_venta},
                           compras_tot={'monto_total': compras_sin_iva + iva_compra, 'iva': iva_compra, 'iva_deducible': iva_compra_creditable},
                           ventas_sin_iva=ventas_sin_iva,
                           compras_sin_iva=compras_sin_iva,
                           ganancia_neta=margen_sin_iva,
                           iva_a_pagar=iva_a_pagar,
                           iva_personal_total=iva_personal_total,
                           iva_personal_credito_empresa=iva_personal_credito_empresa,
                           iva_personal_credito_socios=iva_personal_credito_socios,
                           adeudado_compras=adeudado_compras,
                           adeudado_ventas=adeudado_ventas,
                           per_socio=per_socio)

# --------------------- Dashboard export ---------------------
@app.route('/dashboard/export')
def dashboard_export():
    today = date.today()
    year = int(request.args.get('year', today.year))
    month = int(request.args.get('month', today.month))
    ym = f"{year:04d}-{month:02d}"
    fmt = request.args.get('format', 'csv').lower()

    v = db.session.query(
        func.coalesce(func.sum(Venta.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Venta.iva_21 + Venta.iva_105), 0.0)
    ).filter(Venta.ym == ym).first()
    ventas_sin_iva, iva_venta = float(v[0]), float(v[1])

    c = db.session.query(
        func.coalesce(func.sum(Compra.pesos_sin_iva), 0.0),
        func.coalesce(func.sum(Compra.iva_21 + Compra.iva_105), 0.0)
    ).filter(Compra.ym == ym).first()
    compras_sin_iva, iva_compra = float(c[0]), float(c[1])

    p = db.session.query(func.coalesce(func.sum(CompraPersonal.iva_21 + CompraPersonal.iva_105), 0.0)).filter(CompraPersonal.ym == ym).scalar()
    iva_personal_total = float(p or 0.0)
    iva_personal_creditable = iva_personal_total * 0.5
    iva_compra_creditable = iva_compra + iva_personal_creditable
    margen_sin_iva = ventas_sin_iva - compras_sin_iva
    iva_a_pagar = (iva_venta - iva_compra_creditable)

    adeudado_compras = db.session.query(func.count(Compra.id)).filter((Compra.ym == ym) & (Compra.estado == 'ADEUDADO')).scalar()
    adeudado_ventas = db.session.query(func.count(Venta.id)).filter((Venta.ym == ym) & (Venta.estado == 'ADEUDADO')).scalar()

    resumen = [{
        'YM': ym,
        'Ventas_sin_IVA': round(ventas_sin_iva, 2),
        'IVA_Venta': round(iva_venta, 2),
        'Compras_sin_IVA': round(compras_sin_iva, 2),
        'IVA_Compra': round(iva_compra, 2),
        'IVA_Personal_Total': round(iva_personal_total, 2),
        'IVA_Personal_Creditable': round(iva_personal_creditable, 2),
        'IVA_Compra_Creditable': round(iva_compra_creditable, 2),
        'Margen_sin_IVA': round(margen_sin_iva, 2),
        'IVA_a_Pagar': round(iva_a_pagar, 2),
        'Compras_ADEUDADO': int(adeudado_compras or 0),
        'Ventas_ADEUDADO': int(adeudado_ventas or 0)
    }]

    # Per socio
    ventas_sub = db.session.query(
        Venta.socio_id.label('socio_id'),
        func.sum(Venta.pesos_sin_iva).label('ventas_sin_iva')
    ).filter(Venta.ym == ym).group_by(Venta.socio_id).subquery()
    compras_sub = db.session.query(
        Compra.socio_id.label('socio_id'),
        func.sum(Compra.pesos_sin_iva).label('compras_sin_iva')
    ).filter(Compra.ym == ym).group_by(Compra.socio_id).subquery()
    q = db.session.query(
        Socio.nombre,
        func.coalesce(ventas_sub.c.ventas_sin_iva, 0.0),
        func.coalesce(compras_sub.c.compras_sin_iva, 0.0)
    ).outerjoin(ventas_sub, ventas_sub.c.socio_id == Socio.id
    ).outerjoin(compras_sub, compras_sub.c.socio_id == Socio.id)
    per_socio = []
    for nombre, v_sin, c_sin in q.all():
        per_socio.append({
            'YM': ym,
            'nombre_socio': nombre,
            'Ventas_sin_IVA': round(float(v_sin or 0.0), 2),
            'Compras_sin_IVA': round(float(c_sin or 0.0), 2),
            'Ganancia_neta': round(float((v_sin or 0.0) - (c_sin or 0.0)), 2)
        })

    if fmt == 'xlsx':
        if pd is None:
            return Response('Pandas no instalado en el contenedor', status=500)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as writer:
            pd.DataFrame(resumen).to_excel(writer, index=False, sheet_name=f'Resumen_{ym}')
            pd.DataFrame(per_socio).to_excel(writer, index=False, sheet_name=f'PerSocio_{ym}')
        bio.seek(0)
        return send_file(bio, as_attachment=True,
                         download_name=f'dashboard_{ym}.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        sio = io.StringIO()
        writer = csv.DictWriter(sio, fieldnames=resumen[0].keys())
        writer.writeheader(); writer.writerow(resumen[0])
        data = sio.getvalue()
        return Response(data, mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename=dashboard_{ym}.csv'})

# --------------------- Resumen socio (view + export) ---------------------
@app.route('/resumen-socio', endpoint='resumen_socio')
def resumen_socio_view():
    yms = db.session.query(Compra.ym).distinct().union(db.session.query(Venta.ym)).all()
    ym_list = sorted({r[0] for r in yms if r[0]}, reverse=True)
    ym = request.args.get('ym')
    if not ym or ym not in ym_list:
        ym = ym_list[0] if ym_list else None
    if not ym:
        flash('No hay datos cargados aún. Importá el Excel para ver el resumen.', 'warning')
        return render_template('resumen_socio.html', filas=[], ym='', ym_list=[], p_emp=0, p_ven=0, p_soc=0)
    filas, p_emp, p_ven, p_soc = build_resumen_socio(ym)
    return render_template('resumen_socio.html', filas=filas, ym=ym, ym_list=ym_list, p_emp=p_emp, p_ven=p_ven, p_soc=p_soc)

@app.route('/resumen-socio/export', endpoint='resumen_socio_export')
def resumen_socio_export():
    yms = db.session.query(Compra.ym).distinct().union(db.session.query(Venta.ym)).all()
    ym_list = sorted({r[0] for r in yms if r[0]}, reverse=True)
    ym = request.args.get('ym')
    fmt = request.args.get('format', 'csv').lower()
    if not ym or ym not in ym_list:
        ym = ym_list[0] if ym_list else None
    if not ym:
        return Response('No hay datos para exportar', status=400)
    filas, p_emp, p_ven, p_soc = build_resumen_socio(ym)
    if fmt == 'xlsx':
        if pd is None:
            return Response('Pandas no instalado en el contenedor', status=500)
        df = pd.DataFrame(filas)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=f'Resumen_{ym}')
        bio.seek(0)
        return send_file(bio, as_attachment=True, download_name=f'resumen_socio_{ym}.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        sio = io.StringIO()
        writer = csv.DictWriter(sio, fieldnames=list(filas[0].keys()) if filas else ['YM','nombre_socio','Ganancia_neta','Margen_Empresa','Margen_Vendedor','Margen_Socios','Margen_Otros_Socios','Total_Margenes'])
        writer.writeheader();
        for row in filas:
            writer.writerow(row)
        return Response(sio.getvalue(), mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename=resumen_socio_{ym}.csv'})

# --------------------- Resumen ARCA + Totales ARCA ---------------------

def build_resumen_arca():
    filas = []
    for c in Compra.query.all():
        filas.append({
            'tipo_operacion': 'COMPRA',
            'fecha': c.fecha.strftime('%Y-%m-%d'),
            'tipo_comprobante': c.tipo or '',
            'NRO_FACTURA': c.nro_factura or '',
            'CUIT': c.cuit or '',
            'Denominación': c.proveedor or '',
            'PESOS_SIN_IVA': round(c.pesos_sin_iva or 0.0, 2),
            'IVA_21': round(c.iva_21 or 0.0, 2),
            'IVA_105': round(c.iva_105 or 0.0, 2),
            'TOTAL_CON_IVA': round(c.total_con_iva or 0.0, 2),
            'estado': c.estado or '',
            'origen_destino': c.origen or '',
            'nombre_socio': ''
        })
    for p_ in CompraPersonal.query.all():
        filas.append({
            'tipo_operacion': 'COMPRA',
            'fecha': p_.fecha.strftime('%Y-%m-%d'),
            'tipo_comprobante': p_.tipo or '',
            'NRO_FACTURA': p_.nro_factura or '',
            'CUIT': p_.cuit or '',
            'Denominación': p_.proveedor or '',
            'PESOS_SIN_IVA': 0.0,
            'IVA_21': round(p_.iva_21 or 0.0, 2),
            'IVA_105': round(p_.iva_105 or 0.0, 2),
            'TOTAL_CON_IVA': 0.0,
            'estado': p_.estado or '',
            'origen_destino': p_.origen or '',
            'nombre_socio': ''
        })
    for v in Venta.query.all():
        filas.append({
            'tipo_operacion': 'VENTA',
            'fecha': v.fecha.strftime('%Y-%m-%d'),
            'tipo_comprobante': 'Factura',
            'NRO_FACTURA': v.nro_factura or '',
            'CUIT': v.cuit_venta or '',
            'Denominación': v.cliente or '',
            'PESOS_SIN_IVA': round(v.pesos_sin_iva or 0.0, 2),
            'IVA_21': round(v.iva_21 or 0.0, 2),
            'IVA_105': round(v.iva_105 or 0.0, 2),
            'TOTAL_CON_IVA': round(v.total_con_iva or 0.0, 2),
            'estado': v.estado or '',
            'origen_destino': v.destino or '',
            'nombre_socio': ''
        })
    return filas

@app.route('/resumen-arca')
def resumen_arca():
    filas = build_resumen_arca()
    ym = request.args.get('ym')
    if ym:
        filas = [f for f in filas if f['fecha'].startswith(ym)]
    all_dates = sorted({f['fecha'][:7] for f in build_resumen_arca()})
    return render_template('resumen_arca.html', filas=filas, ym=ym or '', ym_list=all_dates)

@app.route('/resumen-arca/export')
def resumen_arca_export():
    filas = build_resumen_arca()
    ym = request.args.get('ym')
    fmt = request.args.get('format', 'csv').lower()
    if ym:
        filas = [f for f in filas if f['fecha'].startswith(ym)]
    if fmt == 'xlsx':
        if pd is None:
            return Response('Pandas no instalado', status=500)
        df = pd.DataFrame(filas)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Resumen_ARCA')
        bio.seek(0)
        name = f'resumen_arca_{ym or "all"}.xlsx'
        return send_file(bio, as_attachment=True, download_name=name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        sio = io.StringIO()
        if filas:
            writer = csv.DictWriter(sio, fieldnames=filas[0].keys()); writer.writeheader(); writer.writerows(filas)
        return Response(sio.getvalue(), mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename=resumen_arca_{ym or "all"}.csv'})

# --------------------- Totales ARCA ---------------------

def build_totales_arca():
    filas = build_resumen_arca()
    agg = {}
    for f in filas:
        ym = f['fecha'][:7]
        key = (ym, f['tipo_operacion'])
        d = agg.setdefault(key, {'YM': ym, 'tipo_operacion': f['tipo_operacion'], 'PESOS_SIN_IVA': 0.0, 'IVA_21': 0.0, 'IVA_105': 0.0, 'TOTAL_CON_IVA': 0.0})
        d['PESOS_SIN_IVA'] += f['PESOS_SIN_IVA']
        d['IVA_21'] += f['IVA_21']
        d['IVA_105'] += f['IVA_105']
        d['TOTAL_CON_IVA'] += f['TOTAL_CON_IVA']
    filas_out = []
    for (_, _), d in agg.items():
        d['Saldo_Tecnico_IVA'] = round(d['IVA_21'] + d['IVA_105'], 2)
        d['PESOS_SIN_IVA'] = round(d['PESOS_SIN_IVA'], 2)
        d['IVA_21'] = round(d['IVA_21'], 2)
        d['IVA_105'] = round(d['IVA_105'], 2)
        d['TOTAL_CON_IVA'] = round(d['TOTAL_CON_IVA'], 2)
        filas_out.append(d)
    filas_out.sort(key=lambda x: (x['YM'], x['tipo_operacion']))
    return filas_out

@app.route('/totales-arca')
def totales_arca():
    filas = build_totales_arca()
    ym = request.args.get('ym')
    if ym:
        filas = [f for f in filas if f['YM'] == ym]
    ym_list = sorted({f['YM'] for f in build_totales_arca()}, reverse=True)
    return render_template('totales_arca.html', filas=filas, ym=ym or '', ym_list=ym_list)

@app.route('/totales-arca/export')
def totales_arca_export():
    filas = build_totales_arca()
    ym = request.args.get('ym')
    fmt = request.args.get('format', 'csv').lower()
    if ym:
        filas = [f for f in filas if f['YM'] == ym]
    if fmt == 'xlsx':
        if pd is None:
            return Response('Pandas no instalado', status=500)
        df = pd.DataFrame(filas)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Totales_ARCA')
        bio.seek(0)
        name = f'totales_arca_{ym or "all"}.xlsx'
        return send_file(bio, as_attachment=True, download_name=name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        sio = io.StringIO()
        if filas:
            writer = csv.DictWriter(sio, fieldnames=filas[0].keys()); writer.writeheader(); writer.writerows(filas)
        return Response(sio.getvalue(), mimetype='text/csv', headers={'Content-Disposition': f'attachment; filename=totales_arca_{ym or "all"}.csv'})

# --------------------- Import XLS con validación de socio ---------------------
@app.route('/import/xls', methods=['GET', 'POST'])
def import_xls():
    if request.method == 'POST':
        if pd is None:
            flash('Pandas no instalado. Ejecutá: pip install pandas openpyxl', 'danger')
            return redirect(url_for('import_xls'))
        file = request.files.get('file')
        if not file:
            flash('Subí un archivo .xlsx/.xlsm', 'warning')
            return redirect(url_for('import_xls'))
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_XL:
            flash('Formato no soportado', 'warning')
            return redirect(url_for('import_xls'))
        filename = secure_filename(file.filename)
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(path)
        rechazos = []
        try:
            socio_oblig = bool(int(get_param('nombre_socio_obligatorio', 1)))
            # Socios
            try:
                df_soc = pd.read_excel(path, sheet_name='Socios')
                if {'nombre_socio','tipo_socio'}.issubset(df_soc.columns):
                    for _, r in df_soc.iterrows():
                        nombre = str(r['nombre_socio']).strip()
                        if not nombre:
                            continue
                        tipo = str(r['tipo_socio']).strip() if pd.notna(r.get('tipo_socio')) else 'Socio'
                        s = db.session.query(Socio).filter_by(nombre=nombre).first()
                        if not s:
                            s = Socio(nombre=nombre, tipo=tipo)
                            db.session.add(s)
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

            # Compras
            df_c = pd.read_excel(path, sheet_name='FactCompras')
            for _, r in df_c.iterrows():
                try:
                    fecha = r.get('FECHA')
                    fecha = parse_date(fecha) if isinstance(fecha, str) else pd.to_datetime(fecha).date()
                    ym = ym_from_date(fecha)
                    socio_id = get_socio_id(r.get('nombre_socio'))
                    if socio_oblig and not socio_id:
                        rechazos.append({'sheet':'FactCompras','motivo':'nombre_socio inválido/ausente','NRO_FACTURA':r.get('NRO_FACTURA'),'FECHA':str(fecha),'PROVEEDOR':r.get('PROVEEDOR')})
                        continue
                    cobj = Compra(
                        fecha=fecha, ym=ym, proveedor=str(r.get('PROVEEDOR','')),
                        socio_id=socio_id,
                        pesos_sin_iva=float(r.get('PESOS_SIN_IVA') or 0),
                        iva_21=float(r.get('IVA_21') or 0),
                        iva_105=float(r.get('IVA_105') or 0),
                        total_con_iva=float(r.get('TOTAL_CON_IVA') or 0),
                        tipo=str(r.get('TIPO') or ''),
                        nro_factura=str(r.get('NRO_FACTURA') or ''),
                        cuit=str(r.get('CUIT') or ''),
                        origen=str(r.get('ORIGEN') or ''),
                        estado=str(r.get('ESTADO') or 'PAGADO'),
                        descripcion=str(r.get('DETALLE') or '')
                    )
                    db.session.add(cobj)
                except Exception as e:
                    rechazos.append({'sheet':'FactCompras','motivo':str(e)})
            db.session.commit()

            # Compras personales
            try:
                df_p = pd.read_excel(path, sheet_name='FactComprasPers')
                for _, r in df_p.iterrows():
                    try:
                        fecha = r.get('FECHA')
                        if pd.isna(fecha):
                            continue
                        fecha = parse_date(fecha) if isinstance(fecha, str) else pd.to_datetime(fecha).date()
                        ym = ym_from_date(fecha)
                        socio_id = get_socio_id(r.get('nombre_socio'))
                        if socio_oblig and not socio_id:
                            rechazos.append({'sheet':'FactComprasPers','motivo':'nombre_socio inválido/ausente','NRO_FACTURA':r.get('NRO_FACTURA'),'FECHA':str(fecha),'PROVEEDOR':r.get('PROVEEDOR')})
                            continue
                        cp = CompraPersonal(
                            fecha=fecha, ym=ym, proveedor=str(r.get('PROVEEDOR','')),
                            socio_id=socio_id,
                            iva_21=float(r.get('IVA_21') or 0),
                            iva_105=float(r.get('IVA_105') or 0),
                            tipo=str(r.get('TIPO') or ''),
                            nro_factura=str(r.get('NRO_FACTURA') or ''),
                            cuit=str(r.get('CUIT') or ''),
                            origen=str(r.get('ORIGEN') or ''),
                            estado=str(r.get('ESTADO') or 'PAGADO')
                        )
                        db.session.add(cp)
                    except Exception as e:
                        rechazos.append({'sheet':'FactComprasPers','motivo':str(e)})
                db.session.commit()
            except Exception:
                pass

            # Ventas
            df_v = pd.read_excel(path, sheet_name='FactVentas')
            for _, r in df_v.iterrows():
                try:
                    fecha = r.get('FECHA')
                    fecha = parse_date(fecha) if isinstance(fecha, str) else pd.to_datetime(fecha).date()
                    ym = ym_from_date(fecha)
                    socio_id = get_socio_id(r.get('nombre_socio'))
                    if socio_oblig and not socio_id:
                        rechazos.append({'sheet':'FactVentas','motivo':'nombre_socio inválido/ausente','NRO_FACTURA':r.get('NRO_FACTURA'),'FECHA':str(fecha),'CLIENTE':r.get('CLIENTE')})
                        continue
                    vobj = Venta(
                        fecha=fecha, ym=ym, cliente=str(r.get('CLIENTE','')),
                        socio_id=socio_id,
                        pesos_sin_iva=float(r.get('PESOS_SIN_IVA') or 0),
                        iva_21=float(r.get('IVA_21') or 0),
                        iva_105=float(r.get('IVA_105') or 0),
                        total_con_iva=float(r.get('TOTAL_CON_IVA') or 0),
                        nro_factura=str(r.get('NRO_FACTURA') or ''),
                        cuit_venta=str(r.get('CUIT_VENTA') or ''),
                        destino=str(r.get('DESTINO') or ''),
                        estado=str(r.get('ESTADO') or 'PAGADO'),
                        descripcion=str(r.get('DETALLE') or '')
                    )
                    db.session.add(vobj)
                except Exception as e:
                    rechazos.append({'sheet':'FactVentas','motivo':str(e)})
            db.session.commit()

            # Autocompletar márgenes por tipo si faltan
            p_emp = get_param('margen_Empresa', 0.53)
            p_soc = get_param('margen_Socio', 0.09)
            changed = False
            for s in db.session.query(Socio).order_by(Socio.nombre).all():
                if s.margen_porcentaje is None:
                    s.margen_porcentaje = p_emp if s.tipo == 'Empresa' else p_soc
                    changed = True
            if changed:
                db.session.commit()

            if rechazos:
                fname = f"rechazos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                fpath = os.path.join(app.config['UPLOAD_FOLDER'], fname)
                with open(fpath, 'w', newline='', encoding='utf-8') as fh:
                    fieldnames = sorted({k for r in rechazos for k in r.keys()})
                    writer = csv.DictWriter(fh, fieldnames=fieldnames)
                    writer.writeheader(); writer.writerows(rechazos)
                flash(f"Importación completa con {len(rechazos)} filas rechazadas. ", 'warning')
                flash(f"Descargá el detalle: /uploads/{fname}", 'info')
            else:
                flash('Importación desde Excel completa', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Error importando Excel: {e}', 'danger')
        return redirect(url_for('import_xls'))
    return render_template('import_xls.html')

@app.route('/uploads/<path:filename>')
def download_upload(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

# --------------------- Lists ---------------------
@app.route('/socios', methods=['GET', 'POST'])
def socios_view():
    p_emp = get_param('margen_Empresa', 0.53)
    p_soc = get_param('margen_Socio', 0.09)
    if request.method == 'POST':
        nombre = request.form.get('nombre', '').strip()
        tipo = request.form.get('tipo', 'Socio')
        margen = request.form.get('margen_porcentaje')
        margen = float(margen) if margen else (p_emp if tipo=='Empresa' else p_soc)
        if not nombre:
            flash('El nombre es obligatorio', 'danger')
        else:
            if db.session.query(Socio).filter_by(nombre=nombre).first():
                flash('Ya existe un socio con ese nombre', 'warning')
            else:
                s = Socio(nombre=nombre, tipo=tipo, margen_porcentaje=margen)
                db.session.add(s)
                db.session.commit()
                flash('Socio creado', 'success')
        return redirect(url_for('socios_view'))
    changed = False
    for s in db.session.query(Socio).all():
        if s.margen_porcentaje is None:
            s.margen_porcentaje = p_emp if s.tipo == 'Empresa' else p_soc
            changed = True
    if changed:
        db.session.commit()
    socios = db.session.query(Socio).order_by(Socio.nombre).all()
    return render_template('socios_list.html', socios=socios, p_emp=p_emp, p_soc=p_soc)

@app.route('/compras')
def compras_list():
    compras = db.session.query(Compra).order_by(Compra.fecha.desc()).limit(300).all()
    return render_template('compras_list.html', compras=compras)

@app.route('/ventas')
def ventas_list():
    ventas = db.session.query(Venta).order_by(Venta.fecha.desc()).limit(300).all()
    return render_template('ventas_list.html', ventas=ventas)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv('PORT', '5000')))
