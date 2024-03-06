from staging_airflow_python_composer_dag.utils import *

def HTTPSensor_1_1():
    from airflow.providers.http.sensors.http import HttpSensor
    from datetime import timedelta

    return HttpSensor(
        task_id = "HTTPSensor_1_1",
        endpoint = "",
        request_params = None,
        response_check = None,
        http_conn_id = "http_default",
        poke_interval = 60,
    )
