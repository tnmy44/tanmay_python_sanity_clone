from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def sync_dataframe_columns_with_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils.transpiler.dataframe_fcns import syncDataFrameColumnsWithSchema

    return syncDataFrameColumnsWithSchema(in0, spark, columnNames = ["_c5"])
