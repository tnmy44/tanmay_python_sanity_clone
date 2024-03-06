from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def generate_surrogate_keys(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    from prophecy.utils.transpiler.dataframe_fcns import generateSurrogateKeys

    return generateSurrogateKeys(
        in0,
        keyDF = in1,
        naturalKeys = ["_c1"],
        surrogateKey = "_c0",
        overrideSurrogateKeys = "_c4",
        computeOldPortOutput = False,
        spark = spark
    )
