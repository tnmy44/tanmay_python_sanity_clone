from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from python_streaming_main.udfs.UDFs import *

def Reformat_9(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(concat(lit(Config.c_sg1_c_string), lit("a")).alias("c_config"))
