from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_3_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_9_1_1 = Reformat_9_1_1(spark, in0)
    df_Reformat_9_1_1 = collectMetrics(
        spark, 
        df_Reformat_9_1_1, 
        "Subgraph_3_1_1", 
        "8ReBE_rDf6Cv9991es4ZB$$Vwtc_o2Bl1jHLpxQV8viO", 
        "HcKPGkZYMFUdKhp7aJRia$$W7wmsuZkID0jXCncN7z4_", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Subgraph_7_1_1 = Subgraph_7_1_1(spark, subgraph_config.Subgraph_7_1_1, run_id, df_Reformat_9_1_1)
    subgraph_config.update(Config)

    return df_Subgraph_7_1_1
