from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Script_18(spark: SparkSession, in0: DataFrame) -> DataFrame:
    assert Config.c_config_44 == "changed_value_of_config"
    out0 = in0

    return out0
