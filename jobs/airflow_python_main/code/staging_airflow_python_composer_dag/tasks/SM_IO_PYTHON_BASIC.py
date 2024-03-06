from staging_airflow_python_composer_dag.utils import *

@task_wrapper(task_id = "SM_IO_PYTHON_BASIC")
def SM_IO_PYTHON_BASIC(ti=None, params=None, **context):
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "SM_IO_PYTHON_BASIC",
        json = {
          "task_key": "SM_IO_PYTHON_BASIC", 
          "new_cluster": {
            "ssh_public_keys": [], 
            "node_type_id": "m5.xlarge", 
            "spark_version": "13.3.x-scala2.12", 
            "runtime_engine": "STANDARD", 
            "custom_tags": {"billing" : "qa", "qa_usage_type" : "sanity"}, 
            "autoscale": {"min_workers" : 1.0, "max_workers" : 2.0}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/airflow_python_main", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "24", 
              "spark.prophecy.tasks": "{\"SM_IO_PYTHON_BASIC\":\"\"}", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": "false", 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://execution.staging.prophecy.io/eventws"
            }, 
            "init_scripts": [], 
            "driver_node_type_id": "r5.xlarge", 
            "cluster_source": "UI", 
            "aws_attributes": {
              "ebs_volume_count": 1.0, 
              "availability": "SPOT_WITH_FALLBACK", 
              "first_on_demand": 2.0, 
              "ebs_volume_type": "THROUGHPUT_OPTIMIZED_HDD", 
              "spot_bid_price_percent": 99.0, 
              "zone_id": "us-east-1f", 
              "ebs_volume_size": 500.0
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}, 
            "enable_elastic_disk": False
          }, 
          "python_wheel_task": {
            "package_name": "PYTHON_BASIC", 
            "entry_point": "main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.4.0-7.1.78"}},                          {"pypi" : {"package" : "prophecy-libs==1.8.8"}},                          {
                           "whl": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/PYTHON_BASIC-1.0-py3-none-any.whl"
                         },                          {"maven" : {"coordinates" : "mysql:mysql-connector-java:8.0.29", "exclusions" : []}},                          {"pypi" : {"package" : "Theano==1.0.5"}},                          {"pypi" : {"package" : "scipy>=1.6.3,<=1.8.1"}}]
        },
        databricks_conn_id = "sanity_dev",
    )
