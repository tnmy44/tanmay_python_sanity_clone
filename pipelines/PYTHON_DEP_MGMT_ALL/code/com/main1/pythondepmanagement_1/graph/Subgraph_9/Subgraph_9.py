from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_9(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_config_source = config_source(spark)
    df_config_source = collectMetrics(
        spark, 
        df_config_source, 
        "Subgraph_9", 
        "gHwJHSPjemJXBVY49W9Zn$$jcOBkJLo5-FPH6YnRr5ND", 
        "LZR_3V8XoKc9Y_lJ_aiF6$$2ZZLo49k8xfQVyVGUQgJY"
    )
    df_Join_1 = Join_1(spark, df_config_source, in0)
    df_Join_1 = collectMetrics(
        spark, 
        df_Join_1, 
        "Subgraph_9", 
        "-ywrfR7gTUgwaoSMo9bqC$$uR_oFRhLYq8Q5lPbQJ9Fe", 
        "8OGMgBz_XOasZNBLvkoW3$$73fet0m7ARFOczROFhZb-"
    )
    subgraph_config.update(Config)

    return df_Join_1
