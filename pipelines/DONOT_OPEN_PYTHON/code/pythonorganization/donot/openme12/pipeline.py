from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *
from prophecy.utils import *
from pythonorganization.donot.openme12.graph import *

def pipeline(spark: SparkSession) -> None:
    df_src_avro_CustsDatasetInput_1 = src_avro_CustsDatasetInput_1(spark)
    Lookup_1(spark, df_src_avro_CustsDatasetInput_1)
    df_src_avro_CustsDatasetInput = src_avro_CustsDatasetInput(spark)
    df_Limit_1 = Limit_1(spark, df_src_avro_CustsDatasetInput)

    if Config.c_int < - 100:

        if (Config.c_int < - 100):
            df_Removethisbuddy = Removethisbuddy(spark, df_Limit_1)
            df_Removethisbuddy = df_Removethisbuddy.cache()
        else:
            df_Removethisbuddy = df_Limit_1

        df_Deduplicate_1_1 = Deduplicate_1_1(spark, df_Removethisbuddy)
    else:
        df_Deduplicate_1_1 = None

    if Config.c_int > - 100:

        if (Config.c_int > - 100):
            df_SchemaTransform_1 = SchemaTransform_1(spark, df_src_avro_CustsDatasetInput)
        else:
            df_SchemaTransform_1 = df_src_avro_CustsDatasetInput

        df_Reformat_3 = Reformat_3(spark, df_SchemaTransform_1)
    else:
        df_Reformat_3 = None

    if (Config.c_int < - 100):
        df_PassthisBuddy = PassthisBuddy(spark, df_Limit_1)
        df_PassthisBuddy = df_PassthisBuddy.cache()
    else:
        df_PassthisBuddy = df_Limit_1

    if Config.c_int > - 100:
        df_Reformat_4 = Reformat_4(spark, df_SchemaTransform_1)
    else:
        df_Reformat_4 = None

    if Config.c_int > - 100:
        df_CompareColumns_1 = CompareColumns_1(spark, df_SchemaTransform_1, df_Reformat_4)
    else:
        df_CompareColumns_1 = None

    df_DONOT_DELETE = DONOT_DELETE(spark, df_src_avro_CustsDatasetInput)
    df_Deduplicate_1 = Deduplicate_1(spark, df_PassthisBuddy)
    df_Script_1 = Script_1(spark, df_src_avro_CustsDatasetInput)
    df_Script_1 = df_Script_1.cache()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("spark_config1", "spark_config1 value")
    spark.conf.set("spark_config2", "spark_config2 value")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/DONOT_OPEN_PYTHON")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config1", "hadoop_config1 value")
    spark.sparkContext._jsc.hadoopConfiguration().set("hadoop_config2", "hadoop_config2 value")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/DONOT_OPEN_PYTHON", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/DONOT_OPEN_PYTHON")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
