from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def assertion_check(spark: SparkSession, inDF: DataFrame) -> DataFrame:
    from prophecy.utils.transpiler import call_spark_fcn
    from pyspark.sql.types import BooleanType
    inDF\
        .withColumn(
          "check_{}".format(1),
          when(
            (lit(1) == lit(2)).cast(BooleanType()),
            call_spark_fcn("force_error", concat(lit("this assertion will pass"), lit("as condition will fail")))
          )
        )\
        .count()

    return inDF
