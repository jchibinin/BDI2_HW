from kafka import KafkaConsumer
import json
import sqlite3
from datetime import datetime
import time

# Настройка потребителя Kafka
consumer = KafkaConsumer(
    'weather',
    bootstrap_servers=['localhost:9093'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_group'
)

# Подключение к базе данных SQLite
def create_database():
    conn = sqlite3.connect('weather.db')
    cursor = conn.cursor()
    
    # Создание таблицы с уникальным индексом по комбинации полей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            temperature FLOAT,
            condition TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(city, temperature, condition)
        )
    ''')
    conn.commit()
    return conn

def save_weather_data(conn, weather_data):
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO weather_data (city, temperature, condition)
            VALUES (?, ?, ?)
        ''', (
            weather_data['city'],
            weather_data['temperature'],
            weather_data['condition']
        ))
        conn.commit()
        print(f"Сохранены данные: {weather_data}")
    except sqlite3.IntegrityError:
        print(f"Дубликат данных пропущен: {weather_data}")

def main():
    while True:
        try:
            conn = create_database()
            print("Ожидание сообщений...")
            
            print(f"Подписан на топики: {consumer.subscription()}")
            print(f"Назначенные разделы: {consumer.assignment()}")
            
            for message in consumer:
                print(f"Получено сообщение из раздела {message.partition}, смещение {message.offset}")
                weather_data = message.value
                print(weather_data)
                save_weather_data(conn, weather_data)
                
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            print("Переподключение через 5 секунд...")
            time.sleep(5)
        finally:
            conn.close()
    
    consumer.close()  # Закрываем consumer только при полном завершении

if __name__ == "__main__":
    main()