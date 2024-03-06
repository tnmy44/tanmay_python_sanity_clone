from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def Python_2(config_1, config_2, config_3, config_4, ti=None, params=None, **context):

    def return_method():
        print(f"{config_1},{config_2},{config_3}")

    import json
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(task_id = "Python_2", python_callable = return_method, show_return_value_in_logs = True)
