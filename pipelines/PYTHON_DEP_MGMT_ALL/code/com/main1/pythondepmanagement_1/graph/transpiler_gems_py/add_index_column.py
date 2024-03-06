from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def add_index_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils import ProphecyDataFrame

    return ProphecyDataFrame(in0, spark).zipWithIndex(0, 1, "_c1", spark)
