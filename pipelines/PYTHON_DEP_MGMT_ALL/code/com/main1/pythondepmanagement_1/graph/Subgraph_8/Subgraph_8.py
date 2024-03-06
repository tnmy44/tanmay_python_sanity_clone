from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_8(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_Script_14 = Script_14(spark, in0)
    df_Script_14 = collectMetrics(
        spark, 
        df_Script_14, 
        "Subgraph_8", 
        "LILCGsaAyVcrq5sN_rnTw$$yNj073N1JSmbGYNsP5cmQ", 
        "h_LrKUtib2prTyGcdxUY8$$608owhdySsj4Etm9d9T6c"
    )
    df_Script_15 = Script_15(spark, df_Script_14)
    df_Script_15 = collectMetrics(
        spark, 
        df_Script_15, 
        "Subgraph_8", 
        "1Sa0fp285sNMNveWK6l9D$$4aem0FDUiVd9HxqcZzyXa", 
        "xqAUU4sK4Mul7PnxLTiTj$$6Q5V5QfQpHBJaExKCmPn1"
    )
    subgraph_config.update(Config)

    return df_Script_15
