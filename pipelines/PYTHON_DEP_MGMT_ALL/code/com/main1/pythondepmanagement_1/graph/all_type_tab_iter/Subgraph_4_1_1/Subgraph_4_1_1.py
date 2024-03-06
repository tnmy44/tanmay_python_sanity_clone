from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_4_1_1(
        spark: SparkSession,
        subgraph_config: SubgraphConfig,
        run_id: str,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame
) -> DataFrame:
    Config.update(subgraph_config)
    df_Script_2_1_1 = Script_2_1_1(spark, in0, in1, in2)
    df_Script_2_1_1 = collectMetrics(
        spark, 
        df_Script_2_1_1, 
        "Subgraph_4_1_1", 
        "TnQvVCwTikSUTuX8cUeAI$$ndATHZJ9UuV66te7qrPgP", 
        "vQWS2vViFkR3lKoNo6xl5$$9F4sfevfxCZ2LvgUvy3LB", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_2_1 = Reformat_2_1(spark, df_Script_2_1_1)
    df_Reformat_2_1 = collectMetrics(
        spark, 
        df_Reformat_2_1, 
        "Subgraph_4_1_1", 
        "ma44dngA5KDUZa_VMbSj4$$HAmIJ3_I9xgoRRFMbtkDW", 
        "9ILMJnlwdbdHs_ZtGPYbo$$yg_t5ZK3IyF_6UuTI1Bem", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_ComplexCategoryReformat1_1_2 = ComplexCategoryReformat1_1_2(spark, df_Reformat_2_1)
    df_ComplexCategoryReformat1_1_2 = collectMetrics(
        spark, 
        df_ComplexCategoryReformat1_1_2, 
        "Subgraph_4_1_1", 
        "kkK3qxFQL1Diij6WoOZjM$$TTMgqDKzKc1EZMVxYl8V4", 
        "U0wdzr5aLyehRaz3-lQlz$$pUs_zpQutvGR7pfZOjghm", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_CustomLimit_1_1 = CustomLimit_1_1(spark, df_ComplexCategoryReformat1_1_2)
    df_CustomLimit_1_1 = collectMetrics(
        spark, 
        df_CustomLimit_1_1, 
        "Subgraph_4_1_1", 
        "wRnNi_egGZ0TBht66Qc7P$$EVobT0iyKnth83rCai0sB", 
        "qh43W2Ln0WPKX8XG9FIhs$$t7kx3rGG9Z1t8N19WoG84", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Subgraph_3_1_1 = Subgraph_3_1_1(spark, subgraph_config.Subgraph_3_1_1, run_id, df_Script_2_1_1)
    df_CustomReformatGem_1_1 = CustomReformatGem_1_1(spark, df_Subgraph_3_1_1)
    df_CustomReformatGem_1_1 = collectMetrics(
        spark, 
        df_CustomReformatGem_1_1, 
        "Subgraph_4_1_1", 
        "r_uKfIsUmrwmGlinlDVgF$$9mS7VKB8VB349qaUuKIAX", 
        "Y_4MExC27DLFcqVwzO2Qj$$JYQmWAH1rJj5MU9V05WgB", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_very_complex_source = very_complex_source(spark)
    df_very_complex_source = collectMetrics(
        spark, 
        df_very_complex_source, 
        "Subgraph_4_1_1", 
        "y-2togpV5FvHNMQdaAKpi$$SVH_FJ3V681T4INI_fNAb", 
        "6jqO332QhrkA71OKoUcD9$$oJjbfak0toD0kn3zPHnSf", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_FlattenSchema_1_1 = FlattenSchema_1_1(spark, df_very_complex_source)
    df_FlattenSchema_1_1 = collectMetrics(
        spark, 
        df_FlattenSchema_1_1, 
        "Subgraph_4_1_1", 
        "t1BRgmxUPw6hBh2_bZjN-$$v7K3U5_pinGFusnRurSar", 
        "82Ci_GBfNKJUKnCKH1mi8$$zWH3oWQ60SsC6KeZylkd8", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_6_1 = Reformat_6_1(spark, df_FlattenSchema_1_1)
    df_Reformat_6_1 = collectMetrics(
        spark, 
        df_Reformat_6_1, 
        "Subgraph_4_1_1", 
        "xRO-MEdcg5t7hVxYEvxQJ$$vRLOhTGxAdDFR3rxFv69R", 
        "sUlguKaYdFaplAm3Dc0Tn$$G8qk_13fHjpRMyE4CI0FV", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_6_1.cache().count()
    df_Reformat_6_1.unpersist()
    df_Reformat_7_1 = Reformat_7_1(spark, df_very_complex_source)
    df_Reformat_7_1 = collectMetrics(
        spark, 
        df_Reformat_7_1, 
        "Subgraph_4_1_1", 
        "pvZWHrfGe3jhEwx94jD-R$$IbvkMkcXo8jbzOEGeZIVA", 
        "dian386FX1jck7eEg4QVQ$$QQujPyuM_SSDxeCvQoTk2", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_8_1 = Reformat_8_1(spark, df_Reformat_7_1)
    df_Reformat_8_1 = collectMetrics(
        spark, 
        df_Reformat_8_1, 
        "Subgraph_4_1_1", 
        "eulchQfzBJBdBYXwCmqVX$$Tk2wv6Cr80GMUdTEQeic7", 
        "OVkD8GyqSdNE9nncHIE4b$$WSQmRD2fNBiek7n95zfbs", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_Reformat_8_1.cache().count()
    df_Reformat_8_1.unpersist()
    df_CustomReformatGem_1_2 = CustomReformatGem_1_2(spark, df_CustomLimit_1_1)
    df_CustomReformatGem_1_2 = collectMetrics(
        spark, 
        df_CustomReformatGem_1_2, 
        "Subgraph_4_1_1", 
        "OfZVZlEE4rMnntk7Vams1$$rqqKIkee7eT0xsiRzwTVV", 
        "0RFZVyitMMbdQcAWFvQAn$$vHVtFJSgNmE1WOrqMeTgU", 
        run_id = run_id, 
        config = subgraph_config
    )
    df_CustomReformatGem_1_2.cache().count()
    df_CustomReformatGem_1_2.unpersist()
    subgraph_config.update(Config)

    return df_CustomReformatGem_1_1
