from airflow import DAG
from datetime import datetime
import requests
import json
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator


def get_weather_data(ti=None):
    url = "https://api.openweathermap.org/data/3.0/onecall/timemachine?lat=27.855988&lon=34.31488&dt=1711976230&appid=2457cc526d0d374b10e0343f56829b42"
    response = requests.request("GET", url)

    temperature = json.loads(response.text)['main']['temp'] - 273
    location = json.loads(response.text)['name']
    print(f"{location = }, {temperature = }")

    ti.xcom_push(key='temperature', value=temperature)

def choosing_description_weather(ti = None):
    temperature = ti.xcom_pull(key='temperature', task_ids='get_weather_data')
    if temperature >= 15:
        return 'warm_branch'
    return 'cold_branch'

Homework_7 = DAG( 'Homework_7_Naftaliev', description= 'Homework_7',
    schedule_interval= '0 12 * * *' ,
    start_date= datetime(2024, 4, 01),
    catchup= False 
)

get_weather_task = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=Homework_7
)

print_task = BranchPythonOperator(
    task_id='print_task',
    python_callable=choosing_description_weather,
    dag=Homework_7
)

bash_print_warm = BashOperator(task_id = 'warm_branch', bash_command='echo "WARM"', dag = Homework_7)
bash_print_cold = BashOperator(task_id = 'cold_branch', bash_command='echo "COLD"', dag = Homework_7)

get_weather_task >> print_task >> [bash_print_warm, bash_print_cold]
