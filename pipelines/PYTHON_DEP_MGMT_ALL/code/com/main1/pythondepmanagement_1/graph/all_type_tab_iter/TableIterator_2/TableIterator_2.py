from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *


class TableIterator_2(MetaGemExec):

    def __init__(self, config, parent_run_id):
        self.config = config
        super().__init__(True, parent_run_id)

    def execute(self, spark: SparkSession, subgraph_config: SubgraphConfig, run_id: str) -> List[DataFrame]:
        Config.update(subgraph_config)
        df_src_catalog_table_test_catalog_source = src_catalog_table_test_catalog_source(spark)
        df_src_catalog_table_test_catalog_source = collectMetrics(
            spark, 
            df_src_catalog_table_test_catalog_source, 
            "TableIterator_2", 
            "-M_Npejug7IXcm4daRyXW$$dZ-Sov5qftfJpjIBmAPD-", 
            "QBnavPWbTTFUdcVSR3WID$$ijHf7oG5mbDA_b6yDxyjM", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_src_catalog_table_test_catalog_source = src_catalog_table_test_catalog_source(spark)
        df_src_catalog_table_test_catalog_source = collectMetrics(
            spark, 
            df_src_catalog_table_test_catalog_source, 
            "TableIterator_2", 
            "sEnRbIKngxm5hK7t9ufx1$$FST9LSjrD1rDnTPHHOERc", 
            "xT74fvMMxBX5KvoWXV5tp$$FaGw-up1Nj3fMxoT1rK2E", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_multiply_c_int_by_10 = multiply_c_int_by_10(spark, df_src_catalog_table_test_catalog_source)
        df_multiply_c_int_by_10 = collectMetrics(
            spark, 
            df_multiply_c_int_by_10, 
            "TableIterator_2", 
            "Qv33mCy1jBaSeeQ1OuTpC$$PLTEiaWV5mKb5BPJ_-ZWH", 
            "PnTb02G8JeX1NI4NBQEpK$$nb_3gwRxkA1NdBuTMC8lA", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Limit_1 = Limit_1(spark, df_multiply_c_int_by_10)
        df_Limit_1 = collectMetrics(
            spark, 
            df_Limit_1, 
            "TableIterator_2", 
            "9QGlRQm8V7mWiyN-tQscm$$vb_yOnkb7t__-ZR2pOD5H", 
            "h28YfHQ86FsIA2d1kpe0v$$Zh-e-pL1q611qrTo2oIq_", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_join_on_c_int_not_equal_a = join_on_c_int_not_equal_a(
            spark, 
            df_src_catalog_table_test_catalog_source, 
            df_Limit_1
        )
        df_join_on_c_int_not_equal_a = collectMetrics(
            spark, 
            df_join_on_c_int_not_equal_a, 
            "TableIterator_2", 
            "BS0hLYAQMlOAczi-vNCt_$$0swPiKnEwzSzbwDxfhtBc", 
            "4zvZrwPydM0OJDMPpOHD-$$s6KVz7irn0Tu_08IEcOkf", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_limit_to_four = limit_to_four(spark, df_multiply_c_int_by_10)
        df_limit_to_four = collectMetrics(
            spark, 
            df_limit_to_four, 
            "TableIterator_2", 
            "1vqPmVbbYPPH3x301fWdt$$aXUOm1cE0MlEASGSNbP8V", 
            "07xfnsBAeXMAShAAIFLeK$$ceo8N1Sz3K3xD6B4-yeAX", 
            run_id = run_id, 
            config = subgraph_config
        )
        TableIterator_3(subgraph_config.TableIterator_3, run_id).apply(spark, df_limit_to_four)
        subgraph_config.update(Config)

        return list((df_join_on_c_int_not_equal_a, ))

    def apply(self, spark: SparkSession, in0: DataFrame, ) -> DataFrame:
        inDFs = []
        results = []
        conf_to_column = dict(
            [("c_short", "c_short"),  ("c_int", "c_int"),  ("c_long", "c_long"),  ("c_decimal", "c_decimal"),              ("c_float", "c_float"),  ("c_boolean", "c_boolean"),  ("c_double", "c_double"),              ("c_string", "c_string"),  ("c_date", "c_date"),  ("c_timestamp", "c_timestamp"),              ("c_array_int", "c_array_int"),  ("c_array_string", "c_array_string"),              ("c_array_long", "c_array_long"),  ("c_array_boolean", "c_array_boolean"),              ("c_array_date", "c_array_date"),  ("c_array_timestamp", "c_array_timestamp"),              ("c_array_float", "c_array_float"),  ("c_array_decimal", "c_array_decimal"),              ("c_struct", "c_struct")]
        )

        if in0.count() > 5000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        for row in in0.collect():
            update_config = self.config.update_from_row_map(row, conf_to_column)
            _inputs = inDFs
            results.append(self.__run__(spark, update_config, *_inputs))

        return do_union(results)
