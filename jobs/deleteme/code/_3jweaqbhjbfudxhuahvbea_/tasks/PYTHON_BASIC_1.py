from _3jweaqbhjbfudxhuahvbea_.utils import *

@task_wrapper(task_id = "PYTHON_BASIC_1")
def PYTHON_BASIC_1(ti=None, params=None, **context):
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "PYTHON_BASIC_1",
        json = {
          "task_key": "PYTHON_BASIC_1", 
          "new_cluster": {
            "ssh_public_keys": [], 
            "node_type_id": "i3.2xlarge", 
            "spark_version": "13.3.x-scala2.12", 
            "runtime_engine": "STANDARD", 
            "num_workers": 2.0, 
            "data_security_mode": "SINGLE_USER", 
            "custom_tags": {"billing" : "qa", "qa_usage_type" : "sanity"}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/deleteme", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "22", 
              "spark.prophecy.tasks": "{\"SM_DISABLED_PYTHON_BASIC\":\"\",\"SM_IO_PYTHON_BASIC\":\"\",\"PYTHON_BASIC_1\":\"\"}", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": "false", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://execution.staging.prophecy.io/eventws"
            }, 
            "init_scripts": [], 
            "driver_node_type_id": "i3.xlarge", 
            "aws_attributes": {
              "first_on_demand": 3.0, 
              "availability": "SPOT", 
              "zone_id": "auto", 
              "spot_bid_price_percent": 100.0
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}
          }, 
          "python_wheel_task": {
            "package_name": "PYTHON_BASIC", 
            "entry_point": "main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.4.0-7.1.70"}},                          {"pypi" : {"package" : "prophecy-libs==1.8.7"}},                          {
                           "whl": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/PYTHON_BASIC-1.0-py3-none-any.whl"
                         },                          {"maven" : {"coordinates" : "mysql:mysql-connector-java:8.0.29", "exclusions" : []}},                          {"pypi" : {"package" : "Theano==1.0.5"}},                          {"pypi" : {"package" : "scipy>=1.6.3,<=1.8.1"}}]
        },
        databricks_conn_id = "6UPRixJPOMDtv1fGQy9m9",
    )
