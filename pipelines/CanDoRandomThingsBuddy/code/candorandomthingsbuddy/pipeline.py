from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from candorandomthingsbuddy.config.ConfigStore import *
from candorandomthingsbuddy.udfs.UDFs import *
from prophecy.utils import *
from candorandomthingsbuddy.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dataset_cust_in = dataset_cust_in(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/CanDoRandomThingsBuddy")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/CanDoRandomThingsBuddy", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/CanDoRandomThingsBuddy")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
