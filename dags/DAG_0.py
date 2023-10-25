from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

from includes.C_v1_etl_pyspark import etl
from includes.D_v1_write_to_s3 import write_to_s3


args = {
    'owner': 'Eric',
    'start_date': days_ago(1),

}

dag = DAG(
    dag_id = 'dag_test',
    default_args = args,
    schedule_interval = '@daily'
)

#task 1: transform data, and write to parquet

task1 = PythonOperator(
    task_id = 'task_1_transform_data',
    python_callable = etl,
    dag = dag
)

#task 2: write parquet to s3
task2 = PythonOperator(
    task_id = 'task_2_write_to_s3',
    python_callable = write_to_s3,
    dag = dag
)

#Task2 should only start once task1 completes?
task1 >> task2
