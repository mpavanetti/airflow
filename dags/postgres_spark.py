import json
from datetime import datetime
from airflow.decorators import dag, task
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

# Variables
spark_dir = Variable.get("spark_dir")
jars_dir = Variable.get("jars_dir")


@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example','decorator','postgres','spark'])
def postgres_spark():
    
    @task()
    def start():
        return True
    
    # Spark Submit
    postgres_spark = SparkSubmitOperator(
        application=f'{spark_dir}spark_postgres_query.py', 
        task_id="postgres_spark_query",
        jars=f"{jars_dir}postgresql-42.6.0.jar"
    )
    
    @task()
    def finish():
        return True
    
    ext = start() >> postgres_spark >> finish()

etl = postgres_spark()