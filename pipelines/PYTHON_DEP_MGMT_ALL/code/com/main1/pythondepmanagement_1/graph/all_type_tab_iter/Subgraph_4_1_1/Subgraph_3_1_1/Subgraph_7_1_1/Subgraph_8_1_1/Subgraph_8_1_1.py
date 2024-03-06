from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_8_1_1(spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_OrderBy_5_1_1 = OrderBy_5_1_1(spark, in0)
    df_OrderBy_5_1_1 = collectMetrics(
        spark, 
        df_OrderBy_5_1_1, 
        "Subgraph_8_1_1", 
        "nxS6VFhf662zSNyOYlmJg$$S8sII0vQVyQNz-UKMJGFZ", 
        "js_krpN_p0i2oBDe3apVe$$sXf2kza9595kblOgDLchC", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Subgraph_9_1_1 = Subgraph_9_1_1(spark, subgraph_config.Subgraph_9_1_1, run_id, df_OrderBy_5_1_1)
    subgraph_config.update(Config)

    return df_Subgraph_9_1_1
