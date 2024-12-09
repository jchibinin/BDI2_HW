import sqlite3

conn = sqlite3.connect('weather.db')
cursor = conn.cursor()
cursor.execute('SELECT * FROM weather_data')
for row in cursor.fetchall():
    print(row)
conn.close()