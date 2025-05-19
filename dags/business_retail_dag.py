from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id='business_retail_dag',
    schedule=None,
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['business', 'retail'],
) as dag:
    
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_csv_to_gcs',
    src='include/dataset/Online_Retail.csv',
    dst='raw/online_retail.csv',
    bucket='sales-dataset-bq'
    )


# task dependencies
start_task >> upload_csv_to_gcs >> end_task
