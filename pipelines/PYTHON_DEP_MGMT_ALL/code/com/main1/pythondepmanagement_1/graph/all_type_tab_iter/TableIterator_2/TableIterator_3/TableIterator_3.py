from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *


class TableIterator_3(MetaGemExec):

    def __init__(self, config, parent_run_id):
        self.config = config
        super().__init__(True, parent_run_id)

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_src_delta_all_type_no_partition_renamed = src_delta_all_type_no_partition_renamed(spark)
        df_src_delta_all_type_no_partition_renamed = collectMetrics(
            spark, 
            df_src_delta_all_type_no_partition_renamed, 
            "TableIterator_3", 
            "JG5AF6qU1xOpPQialbw-4$$BNglGSJ8AI66rMCv12WuU", 
            "okmAWIcdh_eTdXrnEk4d5$$-9eVTN1yyepf5cxWRvGTK", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_1 = Reformat_1(spark, df_src_delta_all_type_no_partition_renamed)
        df_Reformat_1 = collectMetrics(
            spark, 
            df_Reformat_1, 
            "TableIterator_3", 
            "Vf-URbLaD1LfF62Go9FSO$$9Snnpa8E87I86PYWMYuak", 
            "SdEBFeRieSEuKXX7bOCXc$$vi6B9mjdI_acH2gOPFLNp", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_1.cache().count()
        df_Reformat_1.unpersist()
        subgraph_config.update(Config)

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> None:
        inDFs = []
        conf_to_column = dict([("a", "a")])

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        import multiprocessing
        from multiprocessing.pool import ThreadPool
        from functools import partial

        with ThreadPool(processes = 4) as pool:

            def process_row(row, config, inDFs, spark):
                df1 = config.update_from_row_map(row, conf_to_column)

                return self.__run__(spark, df1, *inDFs)

            partial_process_row = partial(process_row, config = self.config, inDFs = [], spark = spark)
            results = pool.map(partial_process_row, in0.collect())

            return do_union(results)
