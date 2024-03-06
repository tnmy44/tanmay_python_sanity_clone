from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def snow_userpass(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
            "sfUser": "cicdaccount",
            "sfPassword": "CuIqZ!9I32t@",
            "sfDatabase": "QA_DATABASE",
            "sfSchema": "QA_SCHEMA",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": ""
          }
        )\
        .option("dbtable", "all_type_table_smaller")\
        .load()
