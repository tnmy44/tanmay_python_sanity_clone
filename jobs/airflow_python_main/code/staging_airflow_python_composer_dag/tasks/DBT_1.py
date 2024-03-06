from staging_airflow_python_composer_dag.utils import *

def DBT_1():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator
    envs = {}
    dbt_deps_cmd = " deps"
    dbt_props_cmd = ""

    if "/home/airflow/gcs/data":
        envs = {"DBT_PROFILES_DIR" : "/home/airflow/gcs/data"}

    envs["DBT_FULL_REFRESH"] = "true"

    if "run_profile_snowflake":
        dbt_props_cmd = " --profile run_profile_snowflake"
        dbt_deps_cmd = " deps --profile run_profile_snowflake"

    return BashOperator(
        task_id = "DBT_1",
        bash_command = " && ".join(
          ["{} && cd $tmpDir/{}".format(
             (
               "set -euxo pipefail && tmpDir=`mktemp -d` && git clone "
               + "{} --branch {} --single-branch $tmpDir".format(
                 "https://github.com/abhisheks-prophecy/sql_snowflake_public_parent",
                 "main_staging"
               )
             ),
             ""
           ),            "dbt" + dbt_deps_cmd,  "dbt seed" + dbt_props_cmd,  "dbt run" + dbt_props_cmd,            "dbt test" + dbt_props_cmd]
        ),
        env = envs,
        append_env = True,
        retries = 0
    )
