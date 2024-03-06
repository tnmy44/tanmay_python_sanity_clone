from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def CompareRecords_1(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    from prophecy.utils.transpiler.dataframe_fcns import compareRecords

    return compareRecords(
        in0,
        otherDataFrame = in1,
        componentName = "component_name",
        limit = 10,
        sparkSession = spark
    )
