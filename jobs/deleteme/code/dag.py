import os
import sys
import pendulum
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from _3jweaqbhjbfudxhuahvbea_.tasks import PYTHON_BASIC_1, SM_DISABLED_PYTHON_BASIC, SM_IO_PYTHON_BASIC
PROPHECY_RELEASE_TAG = "__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__"

with DAG(
    dag_id = "3JwEAqbhjbFudxhUAHvbEA_", 
    schedule_interval = "0 0/1 * * *", 
    default_args = {"owner" : "Prophecy", "retries" : 0, "ignore_first_depends_on_past" : True, "do_xcom_push" : True, "pool" : "hhFvJ5E5"}, 
    params = {
      'c3': Param(False, type = "boolean", title = """c3"""), 
      'c_object': Param(
        {"c_test" : "dasd", "c_truck" : "asd"}, 
        type = "object", 
        required = ["""c_test""", """c_truck"""], 
        title = """c_object"""
      ), 
      'c1': Param(["1"], type = "array", uniqueItems = False, title = """c1"""), 
      '2c': Param(1, type = "number", title = """2c"""), 
      'c': Param("""asd""", type = "string", pattern = """""", title = """c""")
    }, 
    start_date = pendulum.datetime(2023, 9, 26, tz = "UTC"), 
    end_date = pendulum.datetime(2028, 10, 10, tz = "UTC"), 
    catchup = True, 
    tags = []
) as dag:
    SM_IO_PYTHON_BASIC_op = SM_IO_PYTHON_BASIC()
    SM_DISABLED_PYTHON_BASIC_op = SM_DISABLED_PYTHON_BASIC()
    PYTHON_BASIC_1_op = PYTHON_BASIC_1()
    SM_IO_PYTHON_BASIC_op >> SM_DISABLED_PYTHON_BASIC_op
    SM_DISABLED_PYTHON_BASIC_op >> PYTHON_BASIC_1_op
