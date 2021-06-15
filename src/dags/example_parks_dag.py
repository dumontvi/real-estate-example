from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dq import park_extract_dq
from etl.main import main as etl_main

dag = Dag(...)

start = DummyOperator(task_id="start")
extract_success = DummyOperator(task_id="extract_success")

# Using PythonOperator, but can use any operator like Lambda, Databricks...
## Runs job to pull data from API for Parks Address
extract_parks_addr = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={"job_type": "extract", "class": "APIExtract", "job_args": "data.seattle.gov v5tj-kqhc"},
)

extract_parks_addr_dq = PythonOperator(task_id="python_task", python_callable=park_extract_dq.main)


extract_parks_features = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={"job_type": "extract", "class": "APIExtract", "job_args": "data.seattle.gov j9km-ydkc"},
)

extract_parks_features_dq = PythonOperator(task_id="python_task", python_callable=park_extract_dq.main)


transform_parks_addr = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={"job_type": "transform", "class": "TransformParks", "job_args": "2021-01-01"},
)

transform_parks_dq = PythonOperator(
    task_id="python_task",
    python_callable=park_transform_dq.main,  ## Another transform DQ (Example)
)

## Loads to Reporting Layer
load_parks_addr = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={
        "job_type": "load",
        "class": "RedshiftLoader",
        "job_args": '{"table_name": ...} s3://test_bucket/parks_transformed_file',
    },
)

slack_fail = SlackOperator(...)

(
    start
    >> {{extract_parks_addr, extract_parks_addr_dq}, {extract_parks_features, extract_parks_features_dq}}
    >> {extract_success, slack_fail}
)
extract_success >> transform_parks_addr >> transform_parks_dq >> load_parks_addr
