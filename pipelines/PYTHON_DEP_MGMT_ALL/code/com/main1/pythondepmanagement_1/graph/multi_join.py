from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def multi_join(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame, in3: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.a") != col("in1.C_NUM").cast(IntegerType())), "left_outer")\
        .join(in2.alias("in2"), (col("in1.C_NUM") != col("in2.C_NUM10")), "inner")\
        .join(in3.alias("in3"), (col("in2.C_NUM") != col("in3.C_NUM10")), "inner")\
        .select(col("in0.a").alias("a"), col("in1.C_NUM").alias("C_NUM"), col("in2.C_NUM10").alias("C_NUM10"), col("in2.C_DOUBLE").alias("C_DOUBLE"), col("in2.C_STRING").alias("C_STRING"), col("in2.C_TIMESTAMP").alias("C_TIMESTAMP"), col("in2.C_DATE").alias("C_DATE"), col("in2.C_BOOL").alias("C_BOOL"), col("in3.C_OBJECT").alias("C_OBJECT"))
