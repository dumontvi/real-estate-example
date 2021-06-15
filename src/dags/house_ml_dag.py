from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dq import park_extract_dq
from etl.main import main as etl_main

dag = Dag(...)

start = DummyOperator(task_id="start")

# Using PythonOperator, but can use any operator like Lambda, Databricks...
## Runs job join houses dataset with parks dataset

is_complete_house = ExternalSensor("house_transform_task")
is_complete_park = ExternalSensor("park_transform_task")

house_w_parks_feature_set = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={"job_type": "transform", "class": "TransformHouseWParks", "job_args": "2021-01-01"},
)

house_w_parks_feature_dq = PythonOperator(...)

house_w_parks_predict = PythonOperator(
    task_id="python_task",
    python_callable=etl_main,
    op_kwargs={"job_type": "transform", "class": "HouseWParksPredict", "job_args": "2021-01-01"},
)

house_w_parks_predict_dq = PythonOperator(...)

(
    start
    >> {is_complete_house, is_complete_park}
    >> house_w_parks_feature_set
    >> house_w_parks_feature_dq
    >> house_w_parks_predict
    >> house_w_parks_predict_dq
    >> end
)
