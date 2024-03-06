import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from staging_airflow_python_composer_dag.tasks import (
    Branch_1,
    Branch_1_1,
    DBT_1,
    Email_1,
    Email_1_1,
    Email_2,
    Get_file_format,
    HTTPSensor_1,
    Python_1,
    S3FileSensor_1,
    SM_IO_PYTHON_BASIC,
    Script_1,
    ShellScript,
    Slack_1,
    TaskGroup_1_tg,
    TriggerDag_1
)
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "Staging_Airflow_Python_Composer_dag", 
    schedule_interval = "0 0 2 1 *", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True}, 
    start_date = pendulum.datetime(2023, 7, 3, tz = "UTC"), 
    end_date = pendulum.datetime(2025, 7, 3, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    Get_file_format_op = Get_file_format()
    SM_IO_PYTHON_BASIC_op = SM_IO_PYTHON_BASIC()
    S3FileSensor_1_op = S3FileSensor_1()
    ShellScript_op = ShellScript()
    Branch_1_1_op = Branch_1_1()
    DBT_1_op = DBT_1()
    HTTPSensor_1_op = HTTPSensor_1()
    Email_1_1_op = Email_1_1()
    Email_2_op = Email_2()
    Branch_1_op = Branch_1()
    Script_1_op = Script_1()
    Email_1_op = Email_1()
    Python_1_op = Python_1()
    Slack_1_op = Slack_1()
    TaskGroup_1_op = TaskGroup_1_tg()
    TriggerDag_1_op = TriggerDag_1()
    Get_file_format_op >> SM_IO_PYTHON_BASIC_op
    SM_IO_PYTHON_BASIC_op >> [Branch_1_op, HTTPSensor_1_op, S3FileSensor_1_op, TriggerDag_1_op]
    ShellScript_op >> [Branch_1_1_op, DBT_1_op]
    Branch_1_op >> Script_1_op
    Branch_1_1_op >> [Email_1_1_op, Email_2_op]
    Python_1_op >> TaskGroup_1_op
    HTTPSensor_1_op >> Email_1_op
    Script_1_op >> Slack_1_op
