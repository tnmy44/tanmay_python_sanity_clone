from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from livy_for_airflow_os.config.ConfigStore import *
from livy_for_airflow_os.udfs.UDFs import *
from prophecy.utils import *
from livy_for_airflow_os.graph import *

def pipeline(spark: SparkSession) -> None:
    df_livy_os_dataset = livy_os_dataset(spark)
    df_select_sector = select_sector(spark, df_livy_os_dataset)
    df_print_sector = print_sector(spark, df_select_sector)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Livy_for_airflow_OS")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Livy_for_airflow_OS", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/Livy_for_airflow_OS")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
