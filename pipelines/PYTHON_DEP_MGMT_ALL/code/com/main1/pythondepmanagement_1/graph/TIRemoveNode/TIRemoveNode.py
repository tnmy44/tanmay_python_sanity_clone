from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *


class TIRemoveNode(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__(True)

    def execute(
            self, 
            spark: SparkSession,
            subgraph_config: SubgraphConfig,
            run_id: str,
            out0: DataFrame
    ) -> List[DataFrame]:
        Config.update(subgraph_config)
        out0 = collectMetrics(
            spark, 
            out0, 
            "TIRemoveNode", 
            "zP2ahOJ5AzQcf9IRhsVep$$yYKM0rKLHI680qUzi9EiY", 
            "SuAyvB208dfx0xbxgAs2X$$bIOwwH6KljNWApf0YipOe", 
            run_id = run_id, 
            config = Config
        )
        df_Reformat_34 = Reformat_34(spark, out0)
        df_Reformat_34 = collectMetrics(
            spark, 
            df_Reformat_34, 
            "TIRemoveNode", 
            "gwI9WQjT0flj8BhzTy6T1$$XZ9G8eXXYe5tv7Pc9Gs0s", 
            "s8h9Y6kxVL9BIBbP_LKyV$$fl8Cl0Glhu8HC-hjn1op1", 
            run_id = run_id, 
            config = subgraph_config
        )
        subgraph_config.update(Config)

        return list((df_Reformat_34, df_Reformat_34))

    def apply(self, spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame):
        inDFs = [in1]
        results = []
        conf_to_column = dict(
            [("c_short", "c- short"),  ("c_int", "c  - int"),  ("c_long", "- c long"),  ("c_decimal", "c_decimal  -  "),              ("c_float", "`c_float-__  `"),  ("c_boolean", "c -  boolean _  "),  ("c_double", "c_double"),              ("c_string", "c-string")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
