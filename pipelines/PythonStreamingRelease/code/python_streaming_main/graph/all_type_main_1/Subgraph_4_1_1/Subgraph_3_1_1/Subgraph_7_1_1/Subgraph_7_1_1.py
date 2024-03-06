from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from python_streaming_main.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_7_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_OrderBy_4_1_1 = OrderBy_4_1_1(spark, in0)
    df_Subgraph_8_1_1 = Subgraph_8_1_1(spark, subgraph_config.Subgraph_8_1_1, df_OrderBy_4_1_1)
    df_Reformat_3 = Reformat_3(spark, df_OrderBy_4_1_1)
    subgraph_config.update(Config)

    return df_Subgraph_8_1_1
