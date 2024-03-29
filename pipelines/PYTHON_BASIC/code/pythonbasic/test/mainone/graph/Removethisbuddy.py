from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *

def Removethisbuddy(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(concat(lit("a"), lit("b")).alias("c1"))
