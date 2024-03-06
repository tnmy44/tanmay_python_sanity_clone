from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_3_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_9_1_1 = Reformat_9_1_1(spark, in0)
    df_Reformat_9_1_1 = collectMetrics(
        spark, 
        df_Reformat_9_1_1, 
        "Subgraph_3_1_1", 
        "1eDyEEg0DYJ9sSvXa5k6y$$ItUd1wXxM16QVNLe5Irf8", 
        "jbC8bVGEnnlGcNfStEXGJ$$nRZ6ggG7cIM1lL7uVQogN"
    )
    df_Subgraph_7_1_1 = Subgraph_7_1_1(spark, subgraph_config.Subgraph_7_1_1, df_Reformat_9_1_1)
    subgraph_config.update(Config)

    return df_Subgraph_7_1_1