from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from python_unity_catalog.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_1(
        spark: SparkSession,
        subgraph_config: SubgraphConfig,
        in0: DataFrame,
        in1: DataFrame
) -> (DataFrame, DataFrame):
    Config.update(subgraph_config)
    df_Filter_2 = Filter_2(spark, in1)
    df_Subgraph_2 = Subgraph_2(spark, subgraph_config.Subgraph_2, df_Filter_2)
    df_Reformat_3 = Reformat_3(spark, in0)
    subgraph_config.update(Config)

    return df_Subgraph_2, df_Subgraph_2
