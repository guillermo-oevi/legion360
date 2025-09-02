from sqlalchemy import create_engine, text

# Ruta a tu base SQLite
engine = create_engine("sqlite:///app.db")

with engine.connect() as conn:
    # Eliminar duplicados en compras
    conn.execute(text("""
        DELETE FROM compras
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM compras
            GROUP BY fecha, proveedor, nro_factura, total_con_iva
        );
    """))

    # Eliminar duplicados en ventas
    conn.execute(text("""
        DELETE FROM ventas
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM ventas
            GROUP BY fecha, cliente, nro_factura, total_con_iva
        );
    """))

    # Eliminar duplicados en compras_personales
    conn.execute(text("""
        DELETE FROM compras_personales
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM compras_personales
            GROUP BY fecha, proveedor, nro_factura, iva_21, iva_105
        );
    """))

    conn.commit()

print("✔ Base de datos limpiada correctamente.")
