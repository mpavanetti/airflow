import json
from datetime import datetime

from airflow.decorators import dag, task
@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example','decorator'])
def ETL():
    
    @task()
    def extract():
        return True
    
    @task()
    def transform():
        return True
    
    @task()
    def load():
        return True
    
    ext = extract() >> transform() >> load()

etl = ETL()