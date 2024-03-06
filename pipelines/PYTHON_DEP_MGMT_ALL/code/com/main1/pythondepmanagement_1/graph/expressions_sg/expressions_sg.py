from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def expressions_sg(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Reformat_1 = Reformat_1(spark, in0)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "expressions_sg", 
        "NlxGlP7tGJuEPZUgXCkMk$$ua3TQj0HLE8oed3OkfuJ3", 
        "pLdAQWkczGgYb2aFk4Adb$$4Z8BuRL4EF_rKRIf1fOWy"
    )
    df_SQLStatement_1 = SQLStatement_1(spark, in0)
    df_SQLStatement_1 = collectMetrics(
        spark, 
        df_SQLStatement_1, 
        "expressions_sg", 
        "e1ua5r_RHTBb9Ng-9xjqS$$RBaSueLbAt2s6WJqKSi5-", 
        "jiUBqvoom9e8TQjSo-iX_$$-sGIWIviIc5Ck_0NhQ-cH"
    )
    df_Join_1 = Join_1(spark, df_Reformat_1, df_SQLStatement_1)
    df_Join_1 = collectMetrics(
        spark, 
        df_Join_1, 
        "expressions_sg", 
        "CKSvTnHnRP94AWrhBZpH_$$zHjVtSJRpNs6a8YrMUQkl", 
        "DcEf7E_gotp3wVZJrrVqq$$O6xY3QKfsRtkNt1ttpSU3"
    )
    df_limit_to_11 = limit_to_11(spark, df_Join_1)
    df_limit_to_11 = collectMetrics(
        spark, 
        df_limit_to_11, 
        "expressions_sg", 
        "v3jBkWwu5NmzKZKuNVf3O$$3qK4_c4CBxROGuZt_jVOL", 
        "8xZiHBdgBl515w0dugZAv$$6YNPMqqzT0ApGpJ0MdJ98"
    )
    subgraph_config.update(Config)

    return df_limit_to_11
