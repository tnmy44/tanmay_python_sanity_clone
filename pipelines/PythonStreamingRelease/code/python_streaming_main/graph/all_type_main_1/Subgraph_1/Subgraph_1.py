from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from python_streaming_main.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_9 = Reformat_9(spark, in0)
    df_src_jdbc_mix_all_secrets_1 = src_jdbc_mix_all_secrets_1(spark)
    df_Reformat_1_1 = Reformat_1_1(spark, df_src_jdbc_mix_all_secrets_1)
    subgraph_config.update(Config)

    return df_Reformat_9
