# Получение погоды городов #

## Получение API Key ##
Для получения данных о погоде необходимо зарегистрироваться на openweathermap.org
На странице https://home.openweathermap.org/api_keys получить API ключ

## Запуск  kafka: ##
docker-compose up -d  

## Установка зависимостей: ##
pip install kafka-python, requests

## В отдельных терминалах запускаем: ##
### Запуск producer: ###
python ./producer/producer.py --api-key Ваш API ключ 

### Запуск consumer: ###
python ./consumer/consumer.py

### Получение данных: ###
python ./consumer/getdata.py



