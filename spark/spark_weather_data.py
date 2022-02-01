# Airflow Imports
from airflow.models import Variable

# Spark Imports
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col,explode, element_at, expr,unix_timestamp, to_timestamp, to_date, regexp_replace

if __name__ == '__main__':
    tmp_data_dir = Variable.get("weather_data_tmp_directory")

    # Start Spark Session
    spark = (SparkSession 
        .builder  
        .master("local[2]")
        .appName("weather_data")  
        .getOrCreate())
        
    # Read Data From Weather folder
    df = spark.read.format("json") \
            .option('inferSchema',True) \
            .load(f'{tmp_data_dir}weather/') \
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

    #df_current.show(10)

    # Write df_hourly                            
    df_hourly.write \
    .format('csv') \
    .mode('overwrite') \
    .option('header',False) \
    .option('sep',',') \
    .save(f'{tmp_data_dir}processed/hourly_weather/')

    #df_hourly.show(10)