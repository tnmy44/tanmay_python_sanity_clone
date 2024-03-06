from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_9 = Reformat_9(spark, in0)
    df_Reformat_9 = collectMetrics(
        spark, 
        df_Reformat_9, 
        "Subgraph_1", 
        "CcAnysfagHsdSZVuF1HHT$$tO_lHbPJcZoiOGhVaz3A1", 
        "BlQeawSgop5tLxjjjo39U$$6xpOJdi0iI3b8x_9u40jl"
    )
    df_src_jdbc_mix_all_secrets_1 = src_jdbc_mix_all_secrets_1(spark)
    df_src_jdbc_mix_all_secrets_1 = collectMetrics(
        spark, 
        df_src_jdbc_mix_all_secrets_1, 
        "Subgraph_1", 
        "rZW8F2DJfiPGcBlFbBx6r$$3YkVfRDb-capTnw2dolII", 
        "bvyz6JiRFOW27W1LPG_AL$$kgDOuLw71219ymkhIeH_Y"
    )
    df_Reformat_1_1 = Reformat_1_1(spark, df_src_jdbc_mix_all_secrets_1)
    df_Reformat_1_1 = collectMetrics(
        spark, 
        df_Reformat_1_1, 
        "Subgraph_1", 
        "O18GrnNmRpL6lpeUBg5l7$$QdPidHKhiUJmytlGYJyAw", 
        "1NUpVk7zTGZ6Z-6mMdYmd$$tf-g7bQjbBuQeInxOtS1c"
    )
    df_Reformat_1_1.cache().count()
    df_Reformat_1_1.unpersist()
    subgraph_config.update(Config)

    return df_Reformat_9
