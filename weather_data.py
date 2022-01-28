# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable


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

# DAG Skeleton
with DAG('weather_data', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # DAG Dependencies
    start
    