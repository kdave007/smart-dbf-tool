import sqlite3

conn = sqlite3.connect('src/dbf_test.db')
cursor = conn.cursor()
cursor.execute('SELECT sql FROM sqlite_master WHERE type="table" AND name="CANOTA"')
result = cursor.fetchone()
if result:
    print("CANOTA table SQL:")
    print(result[0])
else:
    print("Table not found")
conn.close()
