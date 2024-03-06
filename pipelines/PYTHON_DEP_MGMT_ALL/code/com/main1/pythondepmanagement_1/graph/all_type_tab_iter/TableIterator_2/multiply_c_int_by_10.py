from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def multiply_c_int_by_10(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select((lit(10) * col("c_int")).alias("a"))
