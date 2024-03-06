from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def subgraph25Ports(
        spark: SparkSession,
        subgraph_config: SubgraphConfig,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame,
        in3: DataFrame,
        in4: DataFrame,
        in5: DataFrame,
        in6: DataFrame,
        in7: DataFrame,
        in8: DataFrame,
        in9: DataFrame,
        in10: DataFrame,
        in11: DataFrame,
        in12: DataFrame,
        in13: DataFrame,
        in14: DataFrame,
        in15: DataFrame,
        in16: DataFrame,
        in17: DataFrame,
        in18: DataFrame,
        in19: DataFrame,
        in20: DataFrame,
        in21: DataFrame,
        in22: DataFrame,
        in23: DataFrame,
        in24: DataFrame,
        in25: DataFrame
) -> (DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame,  DataFrame):
    Config.update(subgraph_config)
    df_script_1 = script_1(
        spark, 
        in2, 
        in3, 
        in4, 
        in5, 
        in6, 
        in7, 
        in8, 
        in9, 
        in10, 
        in11, 
        in12, 
        in13, 
        in14, 
        in15, 
        in16, 
        in17, 
        in18, 
        in22, 
        in23, 
        in24, 
        in25
    )
    df_script_1 = collectMetrics(
        spark, 
        df_script_1, 
        "subgraph25Ports", 
        "E8f1cuhJNnXbn5EojIbtI$$KQF_32hYyB-dPChA5tkCs", 
        "gHn1qMZneTyjBdhL4UTrU$$INDCoSEDzw23XN4qFHDT0"
    )
    df_Reformat_1_1 = Reformat_1_1(spark, in1)
    df_Reformat_1_1 = collectMetrics(
        spark, 
        df_Reformat_1_1, 
        "subgraph25Ports", 
        "dpwa_LcHd_SaEbMs4rjC0$$ylUBP7I7dbDQog1jMrIaB", 
        "LGrCjqp7eebNUUJHP1cwB$$S7uj_UfB-Kpkys-I7p99Y"
    )
    df_Reformat_2_1 = Reformat_2_1(spark, in21)
    df_Reformat_2_1 = collectMetrics(
        spark, 
        df_Reformat_2_1, 
        "subgraph25Ports", 
        "_mslZIo6IsBU6arkBxbzX$$ksMz4NE0r-6yxnHBx_xVy", 
        "jZuVHX26d3FWR_cmWFCVC$$wgDwsvR_UHPGaisdP3UDR"
    )
    df_Reformat_2 = Reformat_2(spark, in19)
    df_Reformat_2 = collectMetrics(
        spark, 
        df_Reformat_2, 
        "subgraph25Ports", 
        "A0uHrfPXCIbL5JZks_hRW$$qq2OWKdm1-d1u1QriRNL8", 
        "ftjj7HriL2elsJI6aveb2$$XfqjpD3HnQbdM-HnyJu5e"
    )
    df_Reformat_1 = Reformat_1(spark, in0)
    df_Reformat_1 = collectMetrics(
        spark, 
        df_Reformat_1, 
        "subgraph25Ports", 
        "W8u1azZpfyqafOjfDfknD$$AIhkLakpC1keCZPNsk87t", 
        "ETh7RdNr_3SiufPB0b5I1$$_phAqBUXxBZQ8CJiIskKP"
    )
    df_Reformat_2_2 = Reformat_2_2(spark, in20)
    df_Reformat_2_2 = collectMetrics(
        spark, 
        df_Reformat_2_2, 
        "subgraph25Ports", 
        "o8rlCNjYsz2903_N92bWP$$JRuaTDKrpCuvyCeQ5JNnN", 
        "hJyO4QmujmViO_GQgdwdp$$B0P1XIB56OXMp_120o_V7"
    )
    subgraph_config.update(Config)

    return (df_Reformat_1, df_Reformat_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1,
           df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1,
           df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1,
           df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_Reformat_1_1, df_script_1,
           df_Reformat_2, df_Reformat_2_2, df_Reformat_2_1)
