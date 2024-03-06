from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def src_snow_Config(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
            "sfUser": Config.SNOW_USERNAME,
            "sfPassword": Config.SNOW_PASSWORD,
            "sfDatabase": "QA_DATABASE",
            "sfSchema": "QA_SCHEMA",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": ""
          }
        )\
        .option("query", "select * from all_type_table_smaller limit 4")\
        .load()
