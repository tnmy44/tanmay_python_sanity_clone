from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def Script_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "Script_1", bash_command = "echo \"test\"", )
