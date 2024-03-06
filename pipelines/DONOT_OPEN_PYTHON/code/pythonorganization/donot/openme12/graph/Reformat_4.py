from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *

def Reformat_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        when(col("first_name").like("%a%"), lit("amar"))\
          .when(col("first_name").like("%b%"), lit("abhiimanyu"))\
          .otherwise(col("first_name"))\
          .alias("first_name"), 
        lit("last").alias("last_name"), 
        col("email_main"), 
        lit("invalid").alias("country_code"), 
        col("full_name")
    )
