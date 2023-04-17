import json
from datetime import datetime
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

notebook_task_params = {
    'existing_cluster_id': '',
    'notebook_task': {
        'notebook_path': '',
    },
}

from airflow.decorators import dag, task
@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example','decorator','databricks','notebook'])
def ETL_Databricks():
    
    @task()
    def extract():
        return True
    
    # Transform
    notebook_task = DatabricksSubmitRunOperator(task_id='notebook_task', json=notebook_task_params)
    
    @task()
    def load():
        return True
    
    ext = extract() >> notebook_task >> load()

etl = ETL_Databricks()