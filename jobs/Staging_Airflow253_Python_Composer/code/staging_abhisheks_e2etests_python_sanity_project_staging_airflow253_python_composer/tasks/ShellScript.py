from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def ShellScript():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(task_id = "ShellScript", bash_command = "ls -ltr", )
