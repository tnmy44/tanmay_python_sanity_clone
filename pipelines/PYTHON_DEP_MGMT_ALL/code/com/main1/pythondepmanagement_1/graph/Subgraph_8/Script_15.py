from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Script_15(spark: SparkSession, in0: DataFrame) -> DataFrame:
    assert Config.subgraph_config == "subgraph_value_in_subgraph_in_script"
    out0 = in0

    return out0
