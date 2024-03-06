from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def data_reformatting(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(col("c_name"), col("c_name")).alias("test_concat2"), 
        concat_ws("-", col("c_name"), col("c_name")).alias("test_concat_ws1"), 
        expr("nvl(c_name, c_address)").alias("test_nvl1"), 
        expr("nvl(NULL, 2)").alias("test_nvl2"), 
        coalesce(lit(None), lit(2)).alias("test_coalesce1"), 
        expr("TRY_CAST('1' AS STRING)").alias("test_try_cast1"), 
        lit(1).cast(StringType()).alias("test_cast1"), 
        (col("c_name").cast(DateType()) + expr("INTERVAL '2 00:00:00' DAY TO SECOND")).alias("test_interval1"), 
        (col("c_name").cast(TimestampType()) + expr("INTERVAL '0 02:00:00' DAY TO SECOND")).alias("test_interval2"), 
        row_number().over(Window.partitionBy(col("c_custkey")).orderBy(col("c_name").asc())).alias("test_window1"), 
        row_number().over(Window.partitionBy(lit(1)).orderBy(col("c_name").asc())).alias("test_window2"), 
        when(col("c_custkey").isNull(), concat(col("c_name"), col("c_address")))\
          .otherwise(ceil(col("c_acctbal").cast(DoubleType())))\
          .alias("test_case_when1"), 
        date_add(lit("2016-07-30"), 1).alias("test_date_add1"), 
        (lit("2016-07-30").cast(DateType()) + lit(5)).alias("test_date_add2"), 
        datediff(lit("2020-02-01").cast(DateType()), lit("2020-01-01").cast(DateType())).alias("test_datediff1")
    )
