# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Importing Python Libraries
from datetime import datetime, timedelta
import time
import json
import os
from pandas import json_normalize
from geopy.geocoders import Nominatim
import csv, sqlite3
import glob
import requests

# Default Arguments and attibutes
default_args ={
    'start_date': datetime.today() - timedelta(days=1),
    'owner': 'Matheus'
}

# Get Current date, subtract 5 days and convert to timestamp
todayLessFiveDays =  datetime.today() - timedelta(days=5)
todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())

# Store last 5 days date into a list
days=[]
i = 1
while i < 6:
  todayLessFiveDays =  datetime.today() - timedelta(days=i)
  todayLessFiveDaysTimestamp = time.mktime(todayLessFiveDays.timetuple())
  days.append(todayLessFiveDaysTimestamp)
  i += 1

# Get Connection from airflow db
api_connection = BaseHook.get_connection("openweathermapApi")

# Get Variables
latitude = Variable.get("weather_data_lat")
longitude = Variable.get("weather_data_lon")
units = Variable.get("weather_data_units")
tmp_data_dir = Variable.get("weather_data_tmp_directory")
weather_data_spark_code = Variable.get("weather_data_spark_code")

# Suggested Locations

suggested_locations = (
      ['30.318878','-81.690173'],
      ['28.538336','-81.379234'],
      ['27.950575','-82.457176'],
      ['25.761681','-80.191788'],
      ['34.052235','-118.243683'],
      ['40.712776','-74.005974'],
      ['41.878113','-87.629799'],
      ['32.776665','-96.796989'],
      ['47.950356','-124.385490'],
      ['36.169941','-115.139832']
)

# weather data api query params
api_params = {
    'lat':suggested_locations[0][0],
    'lon':suggested_locations[0][1],
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
    if not os.path.exists(f'{tmp_data_dir}weather/'):
        os.mkdir(f'{tmp_data_dir}weather/')
    if not os.path.exists(f'{tmp_data_dir}processed/'):
        os.mkdir(f'{tmp_data_dir}processed/')
    if not os.path.exists(f'{tmp_data_dir}processed/current_weather/'):
        os.mkdir(f'{tmp_data_dir}processed/current_weather/')
    if not os.path.exists(f'{tmp_data_dir}processed/hourly_weather/'):
        os.mkdir(f'{tmp_data_dir}processed/hourly_weather/')
    
        
# Extract Weather
def _extract_weather():
    if((Variable.get("weather_data_lat") == None or Variable.get("weather_data_lat") == '') and (Variable.get("weather_data_lon") == None or Variable.get("weather_data_lon") == '')):    
        for latitude, longitude in suggested_locations:
            for day in days:
                # weather data api query params
                api_param = {
                    'lat':latitude,
                    'lon':longitude,
                    'units':units,
                    'dt':int(day),
                    'appid':api_connection.password
                }
                r = requests.get(url = api_connection.host + Variable.get("weather_data_endpoint"), params = api_param)
                data = r.json()
                time = datetime.today().strftime('%Y%m%d%H%M%S%f')
                with open(f"{tmp_data_dir}/weather/weather_output_{time}.json", "w") as outfile:
                    json.dump(data, outfile)
    else:
        for day in days:
                # weather data api query params
                api_param = {
                    'lat':Variable.get("weather_data_lat"),
                    'lon':Variable.get("weather_data_lon"),
                    'units':units,
                    'dt':int(day),
                    'appid':api_connection.password
                }
                r = requests.get(url = api_connection.host + Variable.get("weather_data_endpoint"), params = api_param)
                data = r.json()
                time = datetime.today().strftime('%Y%m%d%H%M%S%f')
                with open(f"{tmp_data_dir}/weather/weather_output_{time}.json", "w") as outfile:
                    json.dump(data, outfile)
        

# Store Location Iterative
def _process_location_csv_iterative():
    if((latitude == None or latitude == '') and (longitude == None or longitude == '')):    
        for lat,long in suggested_locations:
            _store_location_csv(lat,long)
    else:
        _store_location_csv(latitude,longitude)
   
# Processing and Deduplicating Weather API Data
def _store_location_csv(lat,long):
    
    # Invoking geo locator api and getting address from latitude and longitude
    geolocator = Nominatim(user_agent="weather_data")
    location = geolocator.reverse(lat+","+long)
    address = location.raw['address']
    #current = datetime.today().strftime('%Y%m%d%H%M%S%f')
    
    # Process location data
    location_df = json_normalize({
        'latitude':lat,
        'logitude': long,
        'city':address.get('city'),
        'state':address.get('state'),
        'postcode':address.get('postcode'),
        'country':address.get('country')
    })
    
    # Store Location
    location_df.to_csv(f'{tmp_data_dir}location.csv', mode='a', sep=',', index=None, header=False)

# Processed files
def get_current_weather_file():
     for i in glob.glob(f'{tmp_data_dir}processed/current_weather/part-*.csv'):
         return i

def get_hourly_weather_file():
    for i in glob.glob(f'{tmp_data_dir}processed/hourly_weather/part-*.csv'):
        return i
    
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
        response_check=lambda response: True if response.status_code == 200 or response.status_code == 204 else False,
        poke_interval=5,
        timeout=60,
        retries=2,
        mode="reschedule",
        soft_fail=False,
        request_params = api_params
    )
    
    # Api is not available
    api_not_available = PythonOperator(
        task_id='api_not_available',
        python_callable=_notify,
        trigger_rule='one_failed'
    )
    
    # Extract User Records Simple Http Operator
    extracting_weather = PythonOperator(
        task_id='extracting_weather',
        python_callable=_extract_weather,
        trigger_rule='all_success'
    )
    
    # TaskGroup for Creating Postgres tables
    with TaskGroup('create_postgres_tables') as create_postgres_tables:
        
    # Create table Location
        creating_table_location = PostgresOperator(
            task_id='creating_table_location',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE TABLE IF NOT EXISTS location_tmp (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NULL,
                    state VARCHAR(255) NULL,
                    postcode VARCHAR(255) NULL,
                    country VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude)
                );
                
                CREATE TABLE IF NOT EXISTS location (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NULL,
                    state VARCHAR(255) NULL,
                    postcode VARCHAR(255) NULL,
                    country VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude)
                );
                
                '''
        )
        
       # Create Table Requested Weather 
        creating_table_requested_weather = PostgresOperator(
            task_id='creating_table_requested_weather',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE TABLE IF NOT EXISTS current_weather_tmp (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    timezone VARCHAR(255) NOT NULL,
                    requested_datetime VARCHAR(255) NULL,
                    sunrise VARCHAR(255) NULL,
                    sunset VARCHAR(255) NULL,
                    temp VARCHAR(255) NULL,
                    feels_like VARCHAR(255) NULL,
                    pressure VARCHAR(255) NULL,
                    humidity VARCHAR(255) NULL,
                    dew_point VARCHAR(255) NULL,
                    uvi VARCHAR(255) NULL,
                    clouds VARCHAR(255) NULL,
                    visibility VARCHAR(255) NULL,
                    wind_speed VARCHAR(255) NULL,
                    wind_deg VARCHAR(255) NULL,
                    weather_id VARCHAR(255) NULL,
                    weather_main VARCHAR(255) NULL,
                    weather_description VARCHAR(255) NULL,
                    weather_icon VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude,requested_datetime)
                );
                
                CREATE TABLE IF NOT EXISTS current_weather (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    timezone VARCHAR(255) NOT NULL,
                    requested_datetime VARCHAR(255) NULL,
                    sunrise VARCHAR(255) NULL,
                    sunset VARCHAR(255) NULL,
                    temp VARCHAR(255) NULL,
                    feels_like VARCHAR(255) NULL,
                    pressure VARCHAR(255) NULL,
                    humidity VARCHAR(255) NULL,
                    dew_point VARCHAR(255) NULL,
                    uvi VARCHAR(255) NULL,
                    clouds VARCHAR(255) NULL,
                    visibility VARCHAR(255) NULL,
                    wind_speed VARCHAR(255) NULL,
                    wind_deg VARCHAR(255) NULL,
                    weather_id VARCHAR(255) NULL,
                    weather_main VARCHAR(255) NULL,
                    weather_description VARCHAR(255) NULL,
                    weather_icon VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude,requested_datetime)
                );
                
                '''
        )
        
        # Create Table Hourly Weather
        creating_table_hourly_weather = PostgresOperator(
            task_id='creating_table_hourly_weather',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE TABLE IF NOT EXISTS hourly_weather_tmp (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    timezone VARCHAR(255) NOT NULL,
                    datetime VARCHAR(255) NULL,
                    temp VARCHAR(255) NULL,
                    feels_like VARCHAR(255) NULL,
                    pressure VARCHAR(255) NULL,
                    humidity VARCHAR(255) NULL,
                    dew_point VARCHAR(255) NULL,
                    uvi VARCHAR(255) NULL,
                    clouds VARCHAR(255) NULL,
                    visibility VARCHAR(255) NULL,
                    wind_speed VARCHAR(255) NULL,
                    wind_deg VARCHAR(255) NULL,
                    wind_gust VARCHAR(255) NULL,
                    weather_id VARCHAR(255) NULL,
                    weather_main VARCHAR(255) NULL,
                    weather_description VARCHAR(255) NULL,
                    weather_icon VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude,datetime)
                );
                
                CREATE TABLE IF NOT EXISTS hourly_weather (
                    latitude VARCHAR(255) NOT NULL,
                    longitude VARCHAR(255) NOT NULL,
                    timezone VARCHAR(255) NOT NULL,
                    datetime VARCHAR(255) NULL,
                    temp VARCHAR(255) NULL,
                    feels_like VARCHAR(255) NULL,
                    pressure VARCHAR(255) NULL,
                    humidity VARCHAR(255) NULL,
                    dew_point VARCHAR(255) NULL,
                    uvi VARCHAR(255) NULL,
                    clouds VARCHAR(255) NULL,
                    visibility VARCHAR(255) NULL,
                    wind_speed VARCHAR(255) NULL,
                    wind_deg VARCHAR(255) NULL,
                    wind_gust VARCHAR(255) NULL,
                    weather_id VARCHAR(255) NULL,
                    weather_main VARCHAR(255) NULL,
                    weather_description VARCHAR(255) NULL,
                    weather_icon VARCHAR(255) NULL,
                    PRIMARY KEY (latitude,longitude,datetime)
                );
                
                '''
        )
        
    # Truncate Temp Tables    
    with TaskGroup('truncate_temp_table_postgres') as truncate_temp_table_postgres:  
        
        # Truncate location_temp Postgres
        truncate_location_temp_postgres = PostgresOperator(
            task_id='truncate_location_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE location_tmp;
                '''
        )
        
        # Truncate current_weather_temp Postgres
        truncate_current_weather_temp_postgres = PostgresOperator(
            task_id='truncate_current_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE current_weather_tmp;
                '''
        )
        
        # Truncate hourly_weather_temp Postgres
        truncate_hourly_weather_temp_postgres = PostgresOperator(
            task_id='truncate_hourly_weather_temp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    TRUNCATE TABLE hourly_weather_tmp;
                '''
        )

    # Process Location Data
    process_location_csv = PythonOperator(
        task_id='process_location_csv',
        python_callable=_process_location_csv_iterative
    )
     
    # Spark Submit
    spark_process_weather = SparkSubmitOperator(
        application=f'{weather_data_spark_code}', task_id="spark_process_weather"
    )
        
    # TaskGroup for Storing processed data into postgres temp tables
    with TaskGroup('store_processed_temp_data_in_postgres') as store_processed_temp_data_in_postgres:

        store_location_tmp_postgres = PostgresOperator(
            task_id='store_location_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql=f'''
                    COPY location_tmp
                    FROM '{tmp_data_dir}location.csv' 
                    DELIMITER ','
                    ;
                '''
        )
        
        store_current_weather_tmp_postgres = PostgresOperator(
            task_id='store_current_weather_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    COPY current_weather_tmp
                    FROM '%s' 
                    DELIMITER ','
                    ;
                ''' % get_current_weather_file()
        )
        
        store_hourly_weather_tmp_postgres = PostgresOperator(
            task_id='store_hourly_weather_tmp_postgres',
            postgres_conn_id='postgres_default',
            sql='''
                    COPY hourly_weather_tmp
                    FROM '%s' 
                    DELIMITER ','
                    ;
                ''' % get_hourly_weather_file()
        )
        
    # TaskGroup for Storing from temp tables to original tables
    with TaskGroup('copy_from_tmp_table_to_original_table') as copy_from_tmp_table_to_original_table:
        
        copy_location_tmp_to_location = PostgresOperator(
            task_id='copy_location_tmp_to_location',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO location 
                    SELECT * 
                    FROM location_tmp
                    EXCEPT
                    SELECT * 
                    FROM location
                    ON CONFLICT (latitude,longitude) DO NOTHING;
                ''' 
        )
        
        copy_current_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_current_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO current_weather 
                    SELECT * 
                    FROM current_weather_tmp
                    EXCEPT
                    SELECT * 
                    FROM current_weather
                    ON CONFLICT (latitude,longitude,requested_datetime) DO NOTHING;
                ''' 
        )
        
        copy_hourly_weather_tmp_to_current_weather = PostgresOperator(
            task_id='copy_hourly_weather_tmp_to_current_weather',
            postgres_conn_id='postgres_default',
            sql='''
                    INSERT INTO hourly_weather 
                    SELECT * 
                    FROM hourly_weather_tmp
                    EXCEPT
                    SELECT * 
                    FROM hourly_weather
                    ON CONFLICT (latitude,longitude,datetime) DO NOTHING;
                ''' 
        )
        
     # TaskGroup for Creating Postgres Views
    with TaskGroup('create_materialized_views') as create_materialized_views:
        # Create View for DataSet 1
        create_view_dataset_1 = PostgresOperator(
            task_id='create_view_dataset_1',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE OR REPLACE VIEW VW_DATASET_1
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                CAST(hw.datetime AS DATE) AS Date,
                EXTRACT(MONTH FROM CAST(hw.datetime AS DATE)) AS Month,
                MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                GROUP BY City,State,Country,Date,Month
                ORDER BY Date DESC;
                '''
        )
        
        # Create View for DataSet 2
        create_view_dataset_2 = PostgresOperator(
            task_id='create_view_dataset_2',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE OR REPLACE VIEW  VW_DATASET_2
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                CAST(hw.datetime AS DATE) AS Date,
                MAX(CAST(hw.temp AS DECIMAL)) AS Max_Temperature,
                MIN(CAST(hw.temp AS DECIMAL)) AS Min_Temperature,
                ROUND(AVG(CAST(hw.temp AS DECIMAL)),2) AS Average_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(CAST(loc.latitude AS DECIMAL),4) = ROUND(CAST(hw.latitude AS DECIMAL),4)
                AND ROUND(CAST(loc.longitude AS DECIMAL),4) = ROUND(CAST(hw.longitude AS DECIMAL),4)
                GROUP BY City,State,Country,Date
                ORDER BY Date DESC;
                '''
        )
        
    # Pre Cleanup task    
    pre_cleanup= BashOperator(
        task_id='pre_cleanup',
        bash_command=f'rm -rf {tmp_data_dir}'
    )    
    
    # Post Cleanup task    
    post_cleanup= BashOperator(
        task_id='post_cleanup',
        bash_command=f'rm -r {tmp_data_dir}'
    )
    
    # DAG Dependencies
    start >> pre_cleanup >> tmp_data >> check_api >> [extracting_weather,api_not_available]
    extracting_weather >> create_postgres_tables >> truncate_temp_table_postgres >> process_location_csv >> spark_process_weather 
    spark_process_weather >> store_processed_temp_data_in_postgres >> copy_from_tmp_table_to_original_table >> create_materialized_views >> post_cleanup