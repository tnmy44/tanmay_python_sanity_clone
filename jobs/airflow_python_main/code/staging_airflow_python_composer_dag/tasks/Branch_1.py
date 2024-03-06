from staging_airflow_python_composer_dag.utils import *

def Branch_1():

    def abc():
        return "Script_1"

    from datetime import timedelta
    from airflow.operators.python import BranchPythonOperator

    return BranchPythonOperator(task_id = "Branch_1", python_callable = abc, )
