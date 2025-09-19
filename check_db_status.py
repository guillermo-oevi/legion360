# check_db_status.py
import sqlite3
import os

DB_FILENAME = 'app.db'
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_FILENAME)

def check_db_status():
    """
    Se conecta a la base de datos SQLite y verifica su estado:
    - Existencia del archivo.
    - Presencia de las tablas esperadas.
    - Esquema de cada tabla (columnas).
    - Cantidad de registros por tabla.
    """
    print(f"--- Verificando estado de '{DB_PATH}' ---")

    if not os.path.exists(DB_PATH):
        print(f"\n[ERROR] El archivo de la base de datos '{DB_FILENAME}' no fue encontrado en esta carpeta.")
        print("Asegúrate de que el archivo exista o ejecuta la aplicación para que se cree.")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print("\n[OK] Conexión exitosa a la base de datos.")
    except sqlite3.Error as e:
        print(f"\n[ERROR] No se pudo conectar a la base de datos: {e}")
        return

    # 1. Verificar tablas existentes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables_in_db = {row[0] for row in cursor.fetchall()}
    expected_tables = {'socios', 'parametros', 'compras', 'ventas'}

    print("\n--- 1. Verificación de Tablas ---")
    print(f"Tablas encontradas: {', '.join(sorted(tables_in_db)) or 'Ninguna'}")

    missing_tables = expected_tables - tables_in_db
    if missing_tables:
        print(f"[ADVERTENCIA] Faltan las siguientes tablas: {', '.join(sorted(missing_tables))}")
    else:
        print("[OK] Todas las tablas esperadas (`socios`, `parametros`, `compras`, `ventas`) están presentes.")

    # 2. Verificar esquemas y contar registros
    print("\n--- 2. Detalle de Tablas y Registros ---")
    schema_ok = True
    for table_name in sorted(tables_in_db):
        print(f"\nTabla: '{table_name}'")

        # Contar registros
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        print(f"  - Registros: {count}")

        # Obtener esquema
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [info[1] for info in cursor.fetchall()]
        print(f"  - Columnas: {', '.join(columns)}")

        # Verificación específica para la migración de 'ventas'
        if table_name == 'ventas' and 'tipo' not in columns:
            print("  - [ADVERTENCIA] La columna 'tipo' no existe en la tabla 'ventas'.")
            print("    Es necesario ejecutar la migración. Revisa las instrucciones.")
            schema_ok = False

    conn.close()

    print("\n--- 3. Resumen Final ---")
    if not missing_tables and schema_ok:
        print("[OK] El estado de la base de datos parece ser correcto y está actualizada.")
    else:
        print("[ATENCIÓN] Se encontraron problemas en la estructura de la base de datos. Revisa las advertencias anteriores.")

if __name__ == '__main__':
    check_db_status()