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
import csv, sqlite3
import glob

# Spark Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode, element_at, expr,unix_timestamp, to_timestamp, to_date, regexp_replace


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
    if not os.path.exists(f'{tmp_data_dir}/hourly_weather/'):
        os.mkdir(f'{tmp_data_dir}/hourly_weather/')
    if not os.path.exists(f'{tmp_data_dir}processed/'):
        os.mkdir(f'{tmp_data_dir}processed/')
    if not os.path.exists(f'{tmp_data_dir}processed/hourly_weather/'):
        os.mkdir(f'{tmp_data_dir}processed/hourly_weather/')
        
# create a database connection to the SQLite database specified by db_file    
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect(sqlite_connection.host)
    except Error as e:
        print(e)
    return conn

# Processing and Deduplicating Weather API Data
def _store_location_csv():
    
    # Invoking geo locator api and getting address from latitude and longitude
    geolocator = Nominatim(user_agent="weather_data")
    location = geolocator.reverse(latitude+","+longitude)
    #address = location.raw['address']
    address = str(location).split(", ")
    print(address)
    # Process location data
    location_df = json_normalize({
        'latitude':latitude,
        'logitude': longitude,
        'number':address[0],
        'road':address[1],
        'neighbourhood':address[2],
        'city':address[3],
        'county':address[4],
        'state':address[5],
        'postcode':address[6],
        'country':address[7]
    })
    
    # Store Location
    location_df.to_csv(f'{tmp_data_dir}location.csv', sep=',', index=None, header=False)

# Store Location SQLite
def _store_location_sqlite():
    conn = create_connection()
    cur = conn.cursor()
    reader = csv.reader(open(f'{tmp_data_dir}location.csv'))
    cur.executemany('INSERT OR IGNORE INTO location VALUES (?,?,?,?,?,?,?,?,?,?)',reader)
    conn.commit()
 
   
def store_weather_response(ti):
    weather_response = ti.xcom_pull(task_ids=['extracting_weather'])
  
    # Checking if the json array is not empty
    if not len(weather_response):
        raise ValueError('Weather json array is empty')
    weather = weather_response[0]
    return weather

def timestampToDate(ts):
    return datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M')

def _store_requested_weather_csv(ti):
    weather= store_weather_response(ti)
    requested=weather['current']
    
    requested_weather_df = json_normalize({
        'latitude':weather['lat'],
        'longitude':weather['lon'],
        'timezone':weather['timezone'],
        'requested_datetime':timestampToDate(requested['dt']),
        'sunrise':timestampToDate(requested['sunrise']),
        'sunset':timestampToDate(requested['sunset']),
        'temp':requested['temp'],
        'feels_like':requested['feels_like'],
        'pressure':requested['pressure'],
        'humidity':requested['humidity'],
        'dew_point':requested['dew_point'],
        'uvi':requested['uvi'],
        'clouds':requested['clouds'],
        'visibility':requested['visibility'],
        'wind_speed':requested['wind_speed'],
        'wind_deg':requested['wind_deg'],
        'weather_id':requested['weather'][0]['id'],
        'weather_main':requested['weather'][0]['main'],
        'weather_description':requested['weather'][0]['description'],
        'weather_icon':requested['weather'][0]['icon']
    })
    
     # Store Requested
    requested_weather_df.to_csv(f'{tmp_data_dir}requested.csv', sep=',', index=None, header=False)
    

# Store Requested Weather SQLite
def _store_requested_weather_sqlite():
    conn = create_connection()
    cur = conn.cursor()
    reader = csv.reader(open(f'{tmp_data_dir}requested.csv'))
    cur.executemany('INSERT OR IGNORE INTO requested_weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',reader)
    conn.commit()
    
# Store Hourly Weather CSV
def _store_hourly_weather_csv(ti):
    weather= store_weather_response(ti)
    time = str(weather['current']['dt'])
    
    # Write dictonary/json into file for futher processing.
    with open(f"{tmp_data_dir}/hourly_weather/hourly_weather_{time}.json", "w") as outfile:
     json.dump(weather, outfile)
    
# Spark Process hourly weather
def _spark_process_hourly_weather():
    spark = SparkSession \
      .builder  \
      .appName("hourly_data")  \
      .getOrCreate()
      
    df = spark.read.format("json") \
            .option('inferSchema',True) \
            .load(f'{tmp_data_dir}/hourly_weather/') \
            .drop("current","timezone_offset")

    df_stg = df.withColumn('hourly',explode(col('hourly'))) \
                .withColumn("datetime", to_timestamp(expr("hourly.dt")))    \
                .withColumn("temp", expr("hourly.temp")) \
                .withColumn("feels_like", expr("hourly.feels_like")) \
                .withColumn("pressure", expr("hourly.pressure")) \
                .withColumn("humidity", expr("hourly.humidity")) \
                .withColumn("dew_point", expr("hourly.dew_point")) \
                .withColumn("uvi", expr("hourly.uvi")) \
                .withColumn("clouds", expr("hourly.clouds")) \
                .withColumn("visibility", expr("hourly.visibility")) \
                .withColumn("wind_speed", expr("hourly.wind_speed")) \
                .withColumn("wind_deg", expr("hourly.wind_deg")) \
                .withColumn("wind_gust", expr("hourly.wind_gust")) \
                .withColumn("weather_id", expr("hourly.weather.id")) \
                .withColumn("weather_id", element_at(col("weather_id"), 1)) \
                .withColumn("weather_main", expr("hourly.weather.main")) \
                .withColumn("weather_main", element_at(col("weather_main"), 1)) \
                .withColumn("weather_description", expr("hourly.weather.description")) \
                .withColumn("weather_description", element_at(col("weather_description"), 1)) \
                .withColumn("weather_icon", expr("hourly.weather.icon")) \
                .withColumn("weather_icon", element_at(col("weather_icon"), 1)) \
                .drop("hourly")
                
    final_df = df_stg.withColumnRenamed('lat','latitude') \
                .withColumnRenamed('lon','longitude') \
                .coalesce(1)
                
    final_df.write \
    .format('csv') \
    .mode('overwrite') \
    .option('header',False) \
    .option('sep',',') \
    .save(f'{tmp_data_dir}processed/hourly_weather/')

def _store_hourly_processed_csv_to_sqlite():
    conn = create_connection()
    cur = conn.cursor()
    reader = csv.reader(open(glob.glob(f'{tmp_data_dir}processed/hourly_weather/part-*.csv')[0]))
    cur.executemany('INSERT OR IGNORE INTO hourly_weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',reader)
    conn.commit()
    
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
                    number TEXT NULL,
                    road TEXT NULL,
                    neighbourhood TEXT NULL,
                    city TEXT NULL,
                    county TEXT NULL,
                    state TEXT NULL,
                    postcode TEXT NULL,
                    country TEXT NULL,
                    PRIMARY KEY (latitude,longitude)
                );
                '''
        )
        
        creating_table_requested_weather = SqliteOperator(
            task_id='creating_table_requested_weather',
            sqlite_conn_id='db_sqlite',
            sql='''
                CREATE TABLE IF NOT EXISTS requested_weather (
                    latitude TEXT NOT NULL,
                    longitude TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    requested_datetime TEXT NULL,
                    sunrise TEXT NULL,
                    sunset TEXT NULL,
                    temp TEXT NULL,
                    feels_like TEXT NULL,
                    pressure TEXT NULL,
                    humidity TEXT NULL,
                    dew_point TEXT NULL,
                    uvi TEXT NULL,
                    clouds TEXT NULL,
                    visibility TEXT NULL,
                    wind_speed TEXT NULL,
                    wind_deg TEXT NULL,
                    weather_id TEXT NULL,
                    weather_main TEXT NULL,
                    weather_description TEXT NULL,
                    weather_icon TEXT NULL,
                    PRIMARY KEY (latitude,longitude,requested_datetime)
                );
                '''
        )
        
        creating_table_hourly_weather = SqliteOperator(
            task_id='creating_table_hourly_weather',
            sqlite_conn_id='db_sqlite',
            sql='''
                CREATE TABLE IF NOT EXISTS hourly_weather (
                    latitude TEXT NOT NULL,
                    longitude TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    datetime TEXT NULL,
                    temp TEXT NULL,
                    feels_like TEXT NULL,
                    pressure TEXT NULL,
                    humidity TEXT NULL,
                    dew_point TEXT NULL,
                    uvi TEXT NULL,
                    clouds TEXT NULL,
                    visibility TEXT NULL,
                    wind_speed TEXT NULL,
                    wind_deg TEXT NULL,
                    wind_gust TEXT NULL,
                    weather_id TEXT NULL,
                    weather_main TEXT NULL,
                    weather_description TEXT NULL,
                    weather_icon TEXT NULL,
                    PRIMARY KEY (latitude,longitude,datetime)
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
        
        # Store Current Weather Data
        store_requested_weather_csv = PythonOperator(
            task_id='store_requested_weather_csv',
            python_callable=_store_requested_weather_csv
        )
        
        # Store hourly Weather Data
        store_hourly_weather_csv = PythonOperator(
            task_id='store_hourly_weather_csv',
            python_callable=_store_hourly_weather_csv
        )
        
    # TaskGroup for Storing CSV Files into SQLITE tables
    with TaskGroup('storing_csv_to_sqlite') as storing_csv_to_sqlite:
        
        store_location_sqlite=PythonOperator(
            task_id='store_location_sqlite',
            python_callable=_store_location_sqlite
        )
        
        store_requested_weather_sqlite=PythonOperator(
            task_id='store_requested_weather_sqlite',
            python_callable=_store_requested_weather_sqlite
        )
        
    # TaskGroup for Spark Processors
    with TaskGroup('spark_processors') as spark_processors:
        spark_process_hourly_weather=PythonOperator(
            task_id='spark_process_hourly_weather',
            python_callable=_spark_process_hourly_weather
        )
        
    # TaskGroup for Spark Processors
    with TaskGroup('store_processed_data_sqlite') as store_processed_data_sqlite:
        store_hourly_processed_csv_to_sqlite=PythonOperator(
            task_id='store_hourly_processed_csv_to_sqlite',
            python_callable=_store_hourly_processed_csv_to_sqlite
        )
    
    # Cleanup task    
    cleanup= BashOperator(
        task_id='cleanup',
        bash_command=f'rm -r {tmp_data_dir}'
    )
    
    
    # DAG Dependencies
    start >> tmp_data >> check_api >> [extracting_weather,api_not_available]
    extracting_weather >> create_sqlite_tables >> processing_data >> storing_csv_to_sqlite >> spark_processors >> store_processed_data_sqlite >> cleanup