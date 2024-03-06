from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_9_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_10_1_1 = Reformat_10_1_1(spark, in0)
    df_Reformat_10_1_1 = collectMetrics(
        spark, 
        df_Reformat_10_1_1, 
        "Subgraph_9_1_1", 
        "DEXoUTpy9NqeJdmMavCMc$$l7kslWxjzQCxf-SmO3lHI", 
        "BaDG0b0fW3ZvsAJLq_mlc$$iGXzSWSHBYeF6qAQ4mSkK", 
        run_id = run_id, 
        config = subgraph_config
    )
    subgraph_config.update(Config)

    return df_Reformat_10_1_1
