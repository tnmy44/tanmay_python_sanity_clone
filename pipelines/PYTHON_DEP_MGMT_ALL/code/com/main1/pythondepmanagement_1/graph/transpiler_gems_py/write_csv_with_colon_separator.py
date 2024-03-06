from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def write_csv_with_colon_separator(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, array, array_join

    return in0.select(array_join(array(col("*")), ":").alias("_c3"))
