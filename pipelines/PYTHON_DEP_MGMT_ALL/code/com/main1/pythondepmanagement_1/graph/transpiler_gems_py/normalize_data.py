from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def normalize_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils import ProphecyDataFrame

    return ProphecyDataFrame(in0, spark).normalize(lit(2), None, None, "1", [col("_c0")], dict([]), dict([]))
