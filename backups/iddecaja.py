import sqlite3

conn = sqlite3.connect("app.db")
cursor = conn.cursor()

cursor.execute("ALTER TABLE compras ADD COLUMN transaccion_id TEXT;")
cursor.execute("ALTER TABLE ventas ADD COLUMN transaccion_id TEXT;")

conn.commit()
conn.close()
