from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Script_16(spark: SparkSession, in0: DataFrame) -> DataFrame:
    assert Config.Subgraph_8.subgraph_config == "subgraph_value_in_subgraph_in_script"
    assert Config.pipeline_config == "pipeline_config_in_script"
    Config.Subgraph_8.subgraph_config = "subgraph_value_in_pipeline_in_script"
    out0 = in0

    return out0
