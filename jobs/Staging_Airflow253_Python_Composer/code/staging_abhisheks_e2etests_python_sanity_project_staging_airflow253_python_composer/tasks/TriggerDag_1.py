from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

@task_wrapper(task_id = "TriggerDag_1")
def TriggerDag_1(ti=None, params=None, **context):
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from datetime import timedelta

    return TriggerDagRunOperator(
        task_id = "TriggerDag_1",
        trigger_dag_id = "Staging_Shared_Airflow_Composer_Python_dag",
        conf = None,
        reset_dag_run = False, 
        wait_for_completion = False
    )
