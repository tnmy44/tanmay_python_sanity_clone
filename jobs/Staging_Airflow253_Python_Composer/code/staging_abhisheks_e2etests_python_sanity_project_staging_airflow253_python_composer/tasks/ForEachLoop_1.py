from airflow.decorators import task_group
from .ForEachLoop_1_tasks import *
from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

@task_group(group_id = "ForEachLoop_1", default_args = {})
def ForEachLoop_1_tg(value):

    @task(task_id = "Python_2")
    def Python_2_op(value, **context):
        Python_2(value["c1_data"][0]["data"], value["c2_data"][0]["data"], value["c3_data"], value["c3_data"], **context)\
            .execute(context)

    Python_2_call = Python_2_op(value)
