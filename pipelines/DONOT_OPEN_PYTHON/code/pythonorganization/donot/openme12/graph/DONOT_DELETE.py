from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pythonorganization.donot.openme12.config.ConfigStore import *
from pythonorganization.donot.openme12.udfs.UDFs import *

def DONOT_DELETE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    print(f"String: {Config.c_string}, Int: ${Config.c_int}")
    out0 = in0

    return out0
