from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json

default_args = {
    'owner': 'dixon',
    'depends_on_past': False,
    'email': ['dixon2678@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

access_token = Variable.get('access_token')
project_id = Variable.get('project_id')
job_name = Variable.get('job_name')

with DAG(
    'Data-Pipeline',
    default_args=default_args,
    description='DANA testcase',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 8, 21),
    end_date= datetime(2024, 8, 24),
    catchup = False, 
    tags=['dag']
) as dag:

    # Hit JSON to CSV converter API
    # Refer to app.py for more details
    t1 = SimpleHttpOperator(
        task_id="json_toCsv",
        method="GET",
        http_conn_id= "etl_service",
        endpoint="/api/convert_to_csv",
        dag=dag
    )
    
    # Hit the spark processing to parquet API
    t2 = SimpleHttpOperator(
        task_id="csv_toParquet",
        method="GET",
        http_conn_id= "etl_service",
        endpoint="/api/convert_to_parquet",
        dag=dag
    )
    # Hits the upload to GCS endpoint
    # Need to supply the credentials
    t3 = SimpleHttpOperator(
        task_id="upload_to_GCS",
        method="POST",
        http_conn_id="etl_service",
        endpoint="/api/load_to_gcs",
        data=json.dumps({"creds": Variable.get("creds", deserialize_json=True)}), 
        headers={"Content-Type": "application/json"},
        dag=dag
    )

    # Hits the dbt service in Cloud Run jobs
    t4 = SimpleHttpOperator(
        task_id='dbt_run',
        method='POST',
        http_conn_id='google_run_api',
        endpoint=f'/apis/run.googleapis.com/v1/namespaces/{project_id}/jobs/dbt-dana:run',
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}"
        },
        data='',
        dag=dag
    )
t1 >> t2 >> t3 >> t4
