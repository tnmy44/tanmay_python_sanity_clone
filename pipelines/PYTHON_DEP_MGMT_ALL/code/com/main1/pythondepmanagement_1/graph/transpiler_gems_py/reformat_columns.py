from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def reformat_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_c0").cast(IntegerType()).alias("_c0"), 
        col("_c1").cast(IntegerType()).alias("_c1"), 
        col("_c4").cast(IntegerType()).alias("_c4")
    )
