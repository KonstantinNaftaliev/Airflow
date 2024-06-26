# Операторы в Airflow и их применение для ETL
# 1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать рандомное число и печатать его в
# консоль.

# 2. Создайте PythonOperator, который генерирует рандомное число, возводит его в квадрат и выводит в консоль исходное число и результат.

# 3. Сделайте оператор, который отправляет запрос к https://goweather.herokuapp.com/weather/"location" (вместо location используйте ваше местоположение).

# 4. Задайте последовательный порядок выполнения операторов.

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import random

dag = DAG(
    'HW_6_Naftaliev',
    schedule_interval= '0 12 * * *' ,
    start_date=datetime(2024, 3, 28),  
    catchup=False,
)

# BashOperator, генерируем рандомное число, печатаем в консоль
generate_random_number = BashOperator(
    task_id='generate_random_number',
    bash_command='echo $((RANDOM % 100))',  
    dag=dag,
)

# PythonOperator генерируем рандомное число, возводит его в квадрат и выводим в консоль исходное число и результат
def square_random_number():
    random_number = random.randint(1, 100)
    squared_number = random_number ** 2
    print(f"Случайное число: {random_number}, возведенное в квадрат, равно: {squared_number}")

square_random_number_task = PythonOperator(
    task_id='square_random_number',
    python_callable=square_random_number,
    provide_context=True,
    dag=dag,
)

# Задаем оператор для запроса прогноза погоды
def fetch_weather():
    location = "Санкт-Петербург"
    url = f"https://goweather.herokuapp.com/weather/{location}"
    response = requests.get(url)
    weather_data = response.json()
    print(f"Weather in {location}: {weather_data['temperature']}°C, {weather_data['description']}")

fetch_weather_task = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather,
    dag=dag,
)

# Задаем последовательность выполнения задач
generate_random_number >> square_random_number_task >> fetch_weather_task
