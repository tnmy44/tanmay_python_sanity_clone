from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def join_exclude_c_short(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.c_short") != col("in1.`c- short`")), "inner")\
        .select(col("in1.`c- short`").alias("c- short"), col("in1.`c  - int`").alias("c  - int"), col("in1.`- c long`").alias("- c long"), col("in0.c_struct.c_string").alias("c_string"))
