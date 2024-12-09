import json
import random
import time
import requests
from kafka import KafkaProducer
import argparse

# Создаем продюсера Kafka с дополнительными настройками
producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # Добавляем настройки для повторных попыток и таймаутов
    retries=5,  # Количество повторных попыток
    retry_backoff_ms=1000,  # Задержка между попытками (1 секунда)
    request_timeout_ms=30000,  # Таймаут запроса (30 секунд)
    api_version_auto_timeout_ms=5000,  # Таймаут определения версии API
)

# Список городов для получения данных о погоде
cities = ["Moscow", "Saint Petersburg", "Krasnoyarsk", "Sochi", "Vladivostok"]

def get_weather_data(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        temperature = data['main']['temp']
        condition = data['weather'][0]['description']
        return {"city": city, "temperature": temperature, "condition": condition}
    else:
        print(f"Error fetching weather data for {city}: {response.status_code}")
        return None

def send_weather_updates(producer, api_key):
    while True:
        city = random.choice(cities)
        weather_data = get_weather_data(city, api_key)
        if weather_data:
            # Добавляем обработку результата отправки
            future = producer.send('weather', weather_data)
            try:
                record_metadata = future.get(timeout=10)
                print(f"Sent: {weather_data}")
                print(f"Topic: {record_metadata.topic}")
                print(f"Partition: {record_metadata.partition}")
                print(f"Offset: {record_metadata.offset}")
            except Exception as e:
                print(f"Error sending message: {e}")
        time.sleep(5)

def main():
    parser = argparse.ArgumentParser(description='Weather data producer')
    parser.add_argument('--api-key', required=True, help='OpenWeatherMap API key')
    args = parser.parse_args()
    
    try:
        send_weather_updates(producer, args.api_key)
    except KeyboardInterrupt:
        print("Stopped sending weather updates.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
