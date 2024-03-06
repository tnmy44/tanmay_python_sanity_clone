from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def join_by_customer_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in1.customer_id")), "left_semi")\
        .where((col("in0.customer_id") > lit(10)))\
        .select(col("in0.customer_id").alias("customer_id"), col("in0.first_name").alias("first_name"), col("in0.last_name").alias("last_name"), col("in0.phone").alias("phone"), col("in0.email").alias("email"), col("in0.country_code").alias("country_code"), col("in0.account_open_date").alias("account_open_date"), col("in0.account_flags").alias("account_flags"))
