# migrate_add_tipo.py
# Uso:
#   python migrate_add_tipo.py              
#   usa ./app.db
#   python migrate_add_tipo.py C:\ruta\app.db
#   python migrate_add_tipo.py /ruta/app.db

import sqlite3, os, sys, shutil
from datetime import datetime

def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(project_root, 'app.db')

    if not os.path.exists(db_path):
        print(f'[ERROR] No existe el archivo de base de datos: {db_path}')
        sys.exit(1)

    # Backup simple por las dudas
    backup_path = db_path + f'.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    try:
        shutil.copy2(db_path, backup_path)
        print(f'[OK] Backup creado: {backup_path}')
    except Exception as e:
        print(f'[WARN] No se pudo crear el backup automático: {e}')

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Verificar si la tabla existe
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ventas';")
    if not cur.fetchone():
        print("[ERROR] La tabla 'ventas' no existe en la base de datos. ¿Ejecutaste la app para crear las tablas?")
        conn.close()
        sys.exit(1)

    # Verificar columnas existentes
    cur.execute("PRAGMA table_info(ventas);")
    cols = [r[1] for r in cur.fetchall()]
    if 'tipo' in cols:
        print("[OK] La columna 'tipo' ya existe en 'ventas'. Nada que hacer.")
        conn.close()
        return

    # Agregar columna
    try:
        print("[INFO] Agregando columna 'tipo' (TEXT) a 'ventas' ...")
        cur.execute("ALTER TABLE ventas ADD COLUMN tipo TEXT;")
        conn.commit()
        print("[OK] Columna 'tipo' agregada.")
    except Exception as e:
        print(f"[ERROR] Falló el ALTER TABLE: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
