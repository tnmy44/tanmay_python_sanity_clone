from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

@task_wrapper(task_id = "SM_IO_SCALA_BASIC")
def SM_IO_SCALA_BASIC(ti=None, params=None, **context):
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "SM_IO_SCALA_BASIC",
        json = {
          "task_key": "SM_IO_SCALA_BASIC", 
          "new_cluster": {
            "ssh_public_keys": [], 
            "node_type_id": "i3.xlarge", 
            "spark_version": "13.3.x-scala2.12", 
            "runtime_engine": "STANDARD", 
            "num_workers": 1.0, 
            "data_security_mode": "SINGLE_USER", 
            "custom_tags": {"billing" : "qa", "qa_usage_type" : "sanity"}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/Staging_Airflow253_Python_Composer", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "24", 
              "spark.prophecy.tasks": "{\"SM_IO_SCALA_BASIC\":\"\"}", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": "false", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://execution.staging.prophecy.io/eventws"
            }, 
            "init_scripts": [], 
            "aws_attributes": {
              "first_on_demand": 1.0, 
              "availability": "SPOT_WITH_FALLBACK", 
              "zone_id": "auto", 
              "spot_bid_price_percent": 100.0
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}
          }, 
          "python_wheel_task": {
            "package_name": "SamplingModel_DisabledAll", 
            "entry_point": "main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.4.0-7.1.70"}},                          {"pypi" : {"package" : "prophecy-libs==1.8.7"}},                          {
                           "whl": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/SamplingModel_DisabledAll-1.0-py3-none-any.whl"
                         },                          {"maven" : {"coordinates" : "mysql:mysql-connector-java:8.0.29", "exclusions" : []}},                          {"pypi" : {"package" : "Theano==1.0.5"}},                          {"pypi" : {"package" : "scipy>=1.6.3,<=1.8.1"}}]
        },
        databricks_conn_id = "sanity_dev",
    )
