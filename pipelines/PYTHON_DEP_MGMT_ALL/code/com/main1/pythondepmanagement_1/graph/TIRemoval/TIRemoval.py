from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *


class TIRemoval(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__(True)

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str) -> List[DataFrame]:
        Config.update(subgraph_config)
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("a", "a"),  ("C_NUM", "C_NUM"),  ("C_NUM10", "C_NUM10"),  ("C_DOUBLE", "C_DOUBLE"),  ("C_STRING", "C_STRING"),              ("C_TIMESTAMP", "C_TIMESTAMP"),  ("C_DATE", "C_DATE"),  ("C_BOOL", "C_BOOL"),              ("C_OBJECT", "C_OBJECT")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
