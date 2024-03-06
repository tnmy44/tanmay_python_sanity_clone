from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_3_1_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_9_1_1_1 = Reformat_9_1_1_1(spark, in0)
    df_Reformat_9_1_1_1 = collectMetrics(
        spark, 
        df_Reformat_9_1_1_1, 
        "Subgraph_3_1_1_1", 
        "Py5snKGxmPoqSqVssKNYR$$MAuFKlvbzcGXNwSigX8Ry", 
        "h2a4sJDdV5ix2Q-zyNGlZ$$gKt4trIdUaPP9hRk0p0Ad"
    )
    df_Subgraph_7_1_1_1 = Subgraph_7_1_1_1(spark, subgraph_config.Subgraph_7_1_1_1, df_Reformat_9_1_1_1)
    subgraph_config.update(Config)

    return df_Subgraph_7_1_1_1
