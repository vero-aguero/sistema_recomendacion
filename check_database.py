import sqlite3
import os

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

# Conectar a la base de datos
conn = sqlite3.connect('/Users/vaguero/app/data/database.db')

# Crear un objeto cursor para ejecutar consultas
cursor = conn.cursor()

# Verificar si las tablas existen
cursor.execute("SELECT name FROM sqlite_master WHERE type='restaurants';")
tables = cursor.fetchall()
print("Tablas en la base de datos:")
for table in tables:
    print(table[0])

# Verificar la estructura de una tabla específica (por ejemplo, 'restaurants')
table_name = 'restaurants'
cursor.execute(f"PRAGMA table_info({table_name});")
table_info = cursor.fetchall()
print(f"\nEstructura de la tabla {table_name}:")
for info in table_info:
    print(info)

# Mostrar los primeros 10 registros de la tabla 'restaurants'
cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
records = cursor.fetchall()
print(f"\nPrimeros 10 registros de la tabla {table_name}:")
for record in records:
    print(record)

# Contar registros en la tabla 'restaurants'
cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
count = cursor.fetchone()[0]
print(f"\nNúmero total de registros en la tabla {table_name}: {count}")

# Cerrar la conexión
conn.close()
