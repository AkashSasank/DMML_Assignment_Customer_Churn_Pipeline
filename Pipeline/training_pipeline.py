from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from werkzeug.formparser import default_stream_factory

from ingestion.main import ingest_csv, ingest_api

# Source paths
csv_path = "../Dataset/Telco-Customer-Churn.csv"
api_url = "https://my.api.mockaroo.com/users"
# Output path
raw_path = "../Dataset/Raw Data"
storage_path = "../Dataset"

API_HEADERS = {"X-API-Key": "2a258740"}

def start():
    print("heyy")

with DAG(dag_id="Customer Churn Prediction Training",
         description="Pipeline to fetch, preprocess data and train a customer churn model") as dag:

    start = PythonOperator(python_callable=start, task_id="start")

    api_ingestion = PythonOperator(python_callable=ingest_csv, op_kwargs={"api_url": api_url,
                                                                          "headers":API_HEADERS,
                                                                          "output_dir":raw_path},
                                   task_id="api_ingestion")
    csv_ingestion = PythonOperator(python_callable=ingest_csv, op_kwargs={"csv_path": csv_path,
                                                                          "output_dir":raw_path},
                                   task_id="csv_ingestion")


    start >> [api_ingestion, csv_ingestion]