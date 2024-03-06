from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_7_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_OrderBy_4_1_1 = OrderBy_4_1_1(spark, in0)
    df_OrderBy_4_1_1 = collectMetrics(
        spark, 
        df_OrderBy_4_1_1, 
        "Subgraph_7_1_1", 
        "gCqczo8ZBPhHN3KNhOdXu$$i2XPKQJs88dWhBhqEZDZk", 
        "2AubWmI2rmRJFTO6cxv5W$$dUwE_BxXWTUr3Re3YJVxI", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Subgraph_8_1_1 = Subgraph_8_1_1(spark, subgraph_config.Subgraph_8_1_1, run_id, df_OrderBy_4_1_1)
    df_Reformat_3_1 = Reformat_3_1(spark, df_OrderBy_4_1_1)
    df_Reformat_3_1 = collectMetrics(
        spark, 
        df_Reformat_3_1, 
        "Subgraph_7_1_1", 
        "yjSOlTTJ1Fy4NhALVAb9I$$42kLzd7IzgbjQFPXgNJkd", 
        "fsij4lpeGVVoLCKEzjTIT$$wtjOsnJF6-zajRVdY3gLM", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_3_1.cache().count()
    df_Reformat_3_1.unpersist()
    subgraph_config.update(Config)

    return df_Subgraph_8_1_1
