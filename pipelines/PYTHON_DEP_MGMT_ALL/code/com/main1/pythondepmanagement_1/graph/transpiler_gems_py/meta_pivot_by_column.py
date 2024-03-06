from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def meta_pivot_by_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils.transpiler.dataframe_fcns import metaPivot

    return metaPivot(in0, pivotColumns = ["_c8"], nameField = "_c2", valueField = "_c4", sparkSession = spark)
