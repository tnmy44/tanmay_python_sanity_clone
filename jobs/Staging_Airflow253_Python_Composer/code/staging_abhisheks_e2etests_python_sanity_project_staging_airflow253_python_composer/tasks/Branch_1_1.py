from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def Branch_1_1():

    def which_gem_to_run():
        return "Email_2"

    from datetime import timedelta
    from airflow.operators.python import BranchPythonOperator

    return BranchPythonOperator(task_id = "Branch_1_1", python_callable = which_gem_to_run, )
