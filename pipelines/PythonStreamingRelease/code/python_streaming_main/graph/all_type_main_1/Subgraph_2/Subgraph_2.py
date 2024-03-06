from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from python_streaming_main.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_2(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_10 = Reformat_10(spark, in0)
    subgraph_config.update(Config)

    return df_Reformat_10
