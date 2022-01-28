# Import default Apache Airflow Libraries
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator


# Importing Python Libraries
from datetime import datetime, timedelta


# Default Arguments and attibutes
default_args ={
    'start_date': datetime.today() - timedelta(days=1)
}

# DAG Skeleton
with DAG('weather_data', schedule_interval='@daily',default_args=default_args, catchup=False) as dag:
    
    # Start
    start = DummyOperator(
        task_id='Start'
    )
    
    # DAG Dependencies
    start
    