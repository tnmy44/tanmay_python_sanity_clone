from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def join_on_c_int_not_equal_a(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.c_int") != col("in1.a")), "inner")\
        .select(col("in0.c_int").alias("c_int"), concat(col("in1.a"), col("in0.c_int"), lit(Config.c_boolean), lit(Config.c_double), lit(Config.c_struct.c_short))\
        .alias("c_string"), expr(Config.c_test_config).alias("c_config_check"))
