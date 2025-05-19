from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

with DAG(
    dag_id='business_retail_pipeline',
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['gcp', 'dbt', 'retail'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Task 1: Upload CSV to GCS
    upload_csv = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/Online_Retail.csv',
        dst='raw/online_retail.csv',
        bucket='sales-dataset-bq',
        mime_type='text/csv'
    )

    # Task 2: Create BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id='retail_data',
        location='US',
    )

    # Task 3: Load CSV from GCS into BigQuery
    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_csv_to_bq',
        configuration={
            "load": {
                "sourceUris": ["gs://sales-dataset-bq/raw/online_retail.csv"],
                "destinationTable": {
                    "projectId": 'data-engineering-458813',
                    "datasetId": 'retail_data',
                    "tableId": 'online_retail'
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    # Task 4: Run dbt transformation (assumes dbt is installed and project is in dags/dbt)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /home/airflow/gcs/dags/dbt && dbt run',
    )

    end = EmptyOperator(task_id='end')

    # DAG dependencies
    start >> upload_csv >> create_dataset >> load_to_bq >> dbt_run >> end

