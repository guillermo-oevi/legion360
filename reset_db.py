# reset_db.py
import os
import shutil
from datetime import datetime
import sys

# Añadir el directorio actual al path para permitir la importación de 'main'
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "app.db")
BACKUPS_FOLDER = os.path.join(BASE_DIR, "backups")

def reset_database():
    """
    1. Crea un backup del archivo app.db existente.
    2. Elimina el archivo app.db.
    3. Recrea la base de datos con el esquema correcto a partir de los modelos.
    """
    print("--- Iniciando reseteo de la base de datos ---")

    # 1. Backup
    if os.path.exists(DB_PATH):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"manual_reset_backup_{ts}.db"
        backup_path = os.path.join(BACKUPS_FOLDER, backup_filename)
        try:
            os.makedirs(BACKUPS_FOLDER, exist_ok=True)
            shutil.copy2(DB_PATH, backup_path)
            print(f"[OK] Backup creado en: {backup_path}")
        except Exception as e:
            print(f"[ERROR] No se pudo crear el backup: {e}")
            # Detener el proceso si el backup falla
            return
    else:
        print("[INFO] No se encontró 'app.db'. No se necesita hacer backup.")

    # 2. Eliminar
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
            print("[OK] 'app.db' eliminado correctamente.")
        except Exception as e:
            print(f"[ERROR] No se pudo eliminar 'app.db': {e}")
            # Detener si la eliminación falla
            return

    # 3. Recrear
    try:
        print("[INFO] Recreando la base de datos desde los modelos de 'main.py'...")
        from main import app, db
        with app.app_context():
            db.create_all()
        print("[OK] Base de datos recreada con éxito.")
        print("[IMPORTANTE] La nueva 'app.db' está vacía. Deberás re-importar tus datos desde un Excel o Google Sheet.")
    except Exception as e:
        print(f"[ERROR] Falló la recreación de la base de datos: {e}")

    print("\n--- Proceso de reseteo finalizado ---")

if __name__ == "__main__":
    confirm = input("¿Estás seguro de que quieres hacer backup, borrar y recrear 'app.db'? Esta acción no se puede deshacer. (escribe 'si' para confirmar): ")
    if confirm.lower() == 'si':
        reset_database()
    else:
        print("Operación cancelada.")