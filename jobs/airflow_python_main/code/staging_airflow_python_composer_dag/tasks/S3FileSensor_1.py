from staging_airflow_python_composer_dag.utils import *

def S3FileSensor_1():
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    from datetime import timedelta

    return S3KeySensor(
        task_id = "S3FileSensor_1",
        bucket_key = [s.strip() for s in "dags/_scala_mwaa_renamed.zip".split(",") if s.strip()],
        bucket_name = "prophecy-mwaa-243",
        check_fn = lambda files: {f.get('Size', 0) > 104 for f in files},
        aws_conn_id = "aws_default",
        wildcard_match = False,
        verify = True,
    )
