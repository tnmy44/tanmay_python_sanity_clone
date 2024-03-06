from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def RoundRobinPartition_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    from prophecy.utils.transpiler.dataframe_fcns import zipWithIndex

    return (zipWithIndex(in0, 0, 1, "tempId", spark)\
              .select(col("*"), ((floor(col("tempId") / lit(10))) % lit(2)).alias("groupId"))\
              .filter(col("groupId") == lit(0))\
              .drop("groupId")\
              .drop("tempId"),             zipWithIndex(in0, 0, 1, "tempId", spark)\
              .select(col("*"), ((floor(col("tempId") / lit(10))) % lit(2)).alias("groupId"))\
              .filter(col("groupId") == lit(1))\
              .drop("groupId")\
              .drop("tempId"))
