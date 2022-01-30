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
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
import requests

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
sqlite_connection = BaseHook.get_connection("db_sqlite")

# Get Variables
latitude = Variable.get("weather_data_lat")
longitude = Variable.get("weather_data_lon")
units = Variable.get("weather_data_units")
tmp_data_dir = Variable.get("weather_data_tmp_directory")

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
    
        
# create a database connection to the SQLite database specified by db_file    
def create_connection():
    conn = None
    try:
        conn = sqlite3.connect(sqlite_connection.host)
    except Error as e:
        print(e)
    return conn

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

# Store Location SQLite
def _store_location_sqlite():
    conn = create_connection()
    cur = conn.cursor()
    reader = csv.reader(open(f'{tmp_data_dir}location.csv'))
    cur.executemany('INSERT OR IGNORE INTO location VALUES (?,?,?,?,?,?)',reader)
    conn.commit()
 
# Spark Process hourly weather
def _spark_process_weather():
    # Start Spark Session
    spark = SparkSession \
      .builder  \
      .appName("weather_data")  \
      .getOrCreate()
      
    # Read Data From Weather folder
    df = spark.read.format("json") \
            .option('inferSchema',True) \
            .load(f'{tmp_data_dir}/weather/') \
            .drop("timezone_offset")
            
    # Persist Data (MEMORY_AND_DISK) 
    df.persist()
    
    # Add and processc olumns to df_hourly
    df_hourly = df.withColumn('hourly',explode(col('hourly'))) \
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
                .withColumnRenamed('lat','latitude') \
                .withColumnRenamed('lon','longitude') \
                .drop("hourly","current") \
                .coalesce(1)
                
    # Add and process column to df_current
    df_current = df.withColumn("datetime", to_timestamp(expr("current.dt")))    \
                .withColumn("sunrise", to_timestamp(expr("current.sunrise")))    \
                .withColumn("sunset", to_timestamp(expr("current.sunset")))    \
                .withColumn("temp", expr("current.temp")) \
                .withColumn("feels_like", expr("current.feels_like")) \
                .withColumn("pressure", expr("current.pressure")) \
                .withColumn("humidity", expr("current.humidity")) \
                .withColumn("dew_point", expr("current.dew_point")) \
                .withColumn("uvi", expr("current.uvi")) \
                .withColumn("clouds", expr("current.clouds")) \
                .withColumn("visibility", expr("current.visibility")) \
                .withColumn("wind_speed", expr("current.wind_speed")) \
                .withColumn("wind_deg", expr("current.wind_deg")) \
                .withColumn("weather_id", expr("current.weather.id")) \
                .withColumn("weather_id", element_at(col("weather_id"), 1)) \
                .withColumn("weather_main", expr("current.weather.main")) \
                .withColumn("weather_main", element_at(col("weather_main"), 1)) \
                .withColumn("weather_description", expr("current.weather.description")) \
                .withColumn("weather_description", element_at(col("weather_description"), 1)) \
                .withColumn("weather_icon", expr("current.weather.icon")) \
                .withColumn("weather_icon", element_at(col("weather_icon"), 1)) \
                .withColumnRenamed('lat','latitude') \
                .withColumnRenamed('lon','longitude') \
                .drop("hourly","current") \
                .coalesce(1)
                
    # Write df_current            
    df_current.write \
    .format('csv') \
    .mode('overwrite') \
    .option('header',False) \
    .option('sep',',') \
    .save(f'{tmp_data_dir}processed/current_weather/')
    
    # Write df_hourly                            
    df_hourly.write \
    .format('csv') \
    .mode('overwrite') \
    .option('header',False) \
    .option('sep',',') \
    .save(f'{tmp_data_dir}processed/hourly_weather/')
    
    # Unpersist df
    df.unpersist()

# Store Requested Weather SQLite
def _store_requested_weather_sqlite():
    conn = create_connection()
    cur = conn.cursor()
    reader = csv.reader(open(glob.glob(f'{tmp_data_dir}processed/current_weather/part-*.csv')[0]))
    cur.executemany('INSERT OR IGNORE INTO requested_weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',reader)
    conn.commit()

# Store Hourly Wather SQLite
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
                CREATE TABLE IF NOT EXISTS requested_weather (
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

    # TaskGroup for Creating Postgres Views
    with TaskGroup('create_materialized_views') as create_materialized_views:
        # Create View for DataSet 1
        create_view_dataset_1 = PostgresOperator(
            task_id='create_view_dataset_1',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE VIEW IF NOT EXISTS VW_DATASET_1
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                DATE(hw.datetime) AS Date,
                strftime('%m', DATE(hw.datetime)) AS Month,
                MAX(hw.temp) AS Max_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(loc.latitude,4) = hw.latitude
                AND ROUND(loc.longitude,4) = hw.longitude
                GROUP BY City,State,Country,Date,Month
                ORDER BY Max_Temperature DESC;
                '''
        )
        
        # Create View for DataSet 2
        create_view_dataset_2 = PostgresOperator(
            task_id='create_view_dataset_2',
            postgres_conn_id='postgres_default',
            sql='''
                CREATE VIEW IF NOT EXISTS VW_DATASET_2
                AS
                SELECT 
                loc.country AS Country,
                loc.state AS State,
                loc.city AS City,
                DATE(hw.datetime) AS Date,
                MAX(hw.temp) AS Max_Temperature,
                MIN(hw.temp) AS Min_Temperature,
                AVG(hw.temp) AS Average_Temperature
                FROM location loc, hourly_weather hw
                WHERE ROUND(loc.latitude,4) = hw.latitude
                AND ROUND(loc.longitude,4) = hw.longitude
                GROUP BY City,State,Country,Date
                ORDER BY Date DESC;
                '''
        )
        
    # Process Location Data
    process_location_csv = PythonOperator(
        task_id='process_location_csv',
        python_callable=_process_location_csv_iterative
    )
    
    # Spark Process Weather
    spark_process_weather=PythonOperator(
        task_id='spark_process_weather',
        python_callable=_spark_process_weather
    )
        
    # TaskGroup for Spark Processors
    with TaskGroup('store_processed_data_sqlite') as store_processed_data_sqlite:
        
        store_location_sqlite=PythonOperator(
            task_id='store_location_sqlite',
            python_callable=_store_location_sqlite
        )
        
        store_requested_weather_sqlite=PythonOperator(
            task_id='store_requested_weather_sqlite',
            python_callable=_store_requested_weather_sqlite
        )
        
        store_hourly_processed_csv_to_sqlite=PythonOperator(
            task_id='store_hourly_processed_csv_to_sqlite',
            python_callable=_store_hourly_processed_csv_to_sqlite
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
    extracting_weather >> create_postgres_tables >> process_location_csv >> spark_process_weather >> store_processed_data_sqlite >> create_materialized_views >> post_cleanup