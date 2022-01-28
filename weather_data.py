# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator


# Importing Python Libraries
from datetime import datetime, timedelta
import time


# Default Arguments and attibutes
default_args ={
    'start_date': datetime.today() - timedelta(days=1)
}

# Get Current date, subtract 5 days and convert to timestamp
todayLessFiveDays =  datetime.today() - timedelta(days=5)
todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())

# Get Connection from airflow db
connection = BaseHook.get_connection("openweathermapApi")

# weather data api query params
api_params = {
    'lat':Variable.get("weather_data_lat"),
    'lon':Variable.get("weather_data_lon"),
    'dt':int(todayLessFiveDaysTimestamp),
    'appid':connection.password,
}

# Notify, Email
def _notify(ti):
    raise ValueError('Api Not Available')

# DAG Skeleton
with DAG('weather_data', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # Create Http Sensor Operator
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='openweathermapApi',
        endpoint=Variable.get("weather_data_endpoint"),
        method='GET',
        response_check=lambda response: True if response.status_code == 200 else False,
        poke_interval=5,
        timeout=60,
        retries=2,
        mode="reschedule",
        soft_fail=True,
        request_params = api_params
    )
    
    # Api is not available
    api_not_available = PythonOperator(
        task_id='api_not_available',
        python_callable=_notify,
        trigger_rule='one_failed'
    )
    
    # DAG Dependencies
    start >> check_api
    