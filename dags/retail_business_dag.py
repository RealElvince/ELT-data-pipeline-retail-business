from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
RETAIL_DATASET = os.getenv('RETAIL_DATASET')

if not all([PROJECT_ID, BUCKET_NAME, RETAIL_DATASET]):
    raise ValueError("One or more environment variables are missing: PROJECT_ID, BUCKET_NAME, RETAIL_DATASET")

# Define the schema for the CSV file
schema = [
    {"name": "InvoiceNo", "type": "STRING"},
    {"name": "StockCode", "type": "STRING"},
    {"name": "Description", "type": "STRING"},
    {"name": "Quantity", "type": "INTEGER"},
    {"name": "InvoiceDate", "type": "STRING"},  # <--- this avoids parse error
    {"name": "UnitPrice", "type": "FLOAT"},
    {"name": "CustomerID", "type": "STRING"},
    {"name": "Country", "type": "STRING"},
]

with DAG(
    dag_id='business_retail_pipeline',
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['gcp', 'dbt', 'retail'],
) as dag:

    start_task = EmptyOperator(task_id='start')

   

    # Task 1: Create BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id= RETAIL_DATASET,
        project_id=PROJECT_ID,
        location='US',
        exists_ok=True,
    )

    # Task 2: Load CSV from GCS into BigQuery
    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_csv_to_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/retail_sales/Online_Retail.csv"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": RETAIL_DATASET,
                    "tableId": "online_retail"
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "schema": {
                    "fields": schema
                },
            }
        },
    )



    end_task= EmptyOperator(task_id='end')

    # DAG dependencies
    start_task >> create_dataset >> load_to_bq >> end_task

