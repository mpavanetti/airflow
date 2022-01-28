# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.bash import BashOperator


# Importing Python Libraries
from datetime import datetime, timedelta
import time
import json
import os
from pandas import json_normalize
from geopy.geocoders import Nominatim


# Default Arguments and attibutes
default_args ={
    'start_date': datetime.today() - timedelta(days=1)
}

# Get Current date, subtract 5 days and convert to timestamp
todayLessFiveDays =  datetime.today() - timedelta(days=5)
todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())

# Get Connection from airflow db
api_connection = BaseHook.get_connection("openweathermapApi")
sqlite_connection = BaseHook.get_connection("db_sqlite")

# Get Variables
latitude = Variable.get("weather_data_lat")
longitude = Variable.get("weather_data_lon")
units = Variable.get("weather_data_units")
tmp_data_dir = Variable.get("weather_data_tmp_directory")

# weather data api query params
api_params = {
    'lat':latitude,
    'lon':longitude,
    'units':units,
    'dt':int(todayLessFiveDaysTimestamp),
    'appid':api_connection.password,
}

# Notify, Email
def _notify(ti):
    raise ValueError('Api Not Available')

# Tmp Data Check
def _tmp_data():
    # Checking if directories exist
    if not os.path.exists(tmp_data_dir):
        os.mkdir(tmp_data_dir)

# Processing and Deduplicating Weather API Data
def _store_location_csv():
    
    # Invoking geo locator api and getting address from latitude and longitude
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse(latitude+","+longitude)
    address = location.raw['address']

    # Process location data
    location_df = json_normalize({
        'latitude':latitude,
        'logitude': longitude,
        'tourism':address['tourism'],
        'road':address['road'],
        'neighbourhood':address['neighbourhood'],
        'city':address['city'],
        'county':address['county'],
        'state':address['state'],
        'postcode':address['postcode'],
        'country':address['country'],
        'country_code':address['country_code']
    })
    
    # Store Location
    location_df.to_csv(f'{tmp_data_dir}location.csv', sep='|', index=None, header=False)

# DAG Skeleton
with DAG('weather_data', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # Temp Data 
    tmp_data = PythonOperator(
        task_id='tmp_data',
        python_callable=_tmp_data
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
    
    # Extract User Records Simple Http Operator
    extracting_weather = SimpleHttpOperator(
        task_id='extracting_weather',
        http_conn_id='openweathermapApi',
        endpoint='data/2.5/onecall/timemachine',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        data = api_params,
        log_response=True,
        trigger_rule='all_success'
    )
    
    # TaskGroup for Creating SQLITE tables
    with TaskGroup('create_sqlite_tables') as create_sqlite_tables:
        
    # Create table Location
        creating_table_location = SqliteOperator(
            task_id='creating_table_location',
            sqlite_conn_id='db_sqlite',
            sql='''
                CREATE TABLE IF NOT EXISTS location (
                    latitude TEXT NOT NULL,
                    longitude TEXT NOT NULL,
                    tourism TEXT NULL,
                    road TEXT NULL,
                    neighbourhood TEXT NULL,
                    city TEXT NULL,
                    county TEXT NULL,
                    state TEXT NULL,
                    postcode TEXT NULL,
                    country TEXT NULL,
                    country_code TEXT NULL,
                    PRIMARY KEY (latitude,longitude)
                );
                '''
        )
    
    # TaskGroup for Processing Data
    with TaskGroup('processing_data') as processing_data:
        
        # Store Location Data
        store_location_csv = PythonOperator(
            task_id='store_location_csv',
            python_callable=_store_location_csv
        )
        
    # TaskGroup for Storing CSV Files into SQLITE tables
    with TaskGroup('storing_csv_to_sqlite') as storing_csv_to_sqlite:
        
        storing_location= BashOperator(
        task_id='storing_location',
        bash_command=f'echo -e ".separator "\|"\n.import {tmp_data_dir}location.csv location" | sqlite3 {sqlite_connection.host}'
    )
    
    
    # DAG Dependencies
    start >> tmp_data >> check_api >> [extracting_weather,api_not_available]
    extracting_weather >> create_sqlite_tables >> processing_data >> storing_csv_to_sqlite
    