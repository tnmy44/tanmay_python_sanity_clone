from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *


class all_type_tab_iter(MetaGemExec):

    def __init__(self, config):
        self.config = config
        super().__init__(True)

    def execute(
            self, 
            spark: SparkSession,
            subgraph_config: SubgraphConfig,
            run_id: str,
            out0: DataFrame,
            out1: DataFrame
    ) -> List[DataFrame]:
        Config.update(subgraph_config)
        out0 = collectMetrics(
            spark, 
            out0, 
            "all_type_tab_iter", 
            "AAPrassZb1b2esN17vFBZ$$jy5laQnjmff1rzIWwQN5s", 
            "1NAj1DYmXAQgpJfiJIyPQ$$t9bV2hW0KVicRA8cyNteC", 
            run_id = run_id, 
            config = Config
        )
        out1 = collectMetrics(
            spark, 
            out1, 
            "all_type_tab_iter", 
            "AAPrassZb1b2esN17vFBZ$$jy5laQnjmff1rzIWwQN5s", 
            "4bCkLjD1KgQDJ07mcsLsg$$Gh6b1x1gwqV4C3HoPqU97", 
            run_id = run_id, 
            config = Config
        )
        df_src_parquet_all_type_and_partition_withspacehyphens_renamed_1 = src_parquet_all_type_and_partition_withspacehyphens_renamed_1(
            spark
        )
        df_src_parquet_all_type_and_partition_withspacehyphens_renamed_1 = collectMetrics(
            spark, 
            df_src_parquet_all_type_and_partition_withspacehyphens_renamed_1, 
            "all_type_tab_iter", 
            "sFovB1hMrEorBgENvJreP$$6D39Gh9rM-rsoCPFNpu01", 
            "9zUbv7gey_hWyOnxdbdm2$$ER9Skkq28hEdveCKU-3OO", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_limit_to_5_1 = limit_to_5_1(spark, df_src_parquet_all_type_and_partition_withspacehyphens_renamed_1)
        df_limit_to_5_1 = collectMetrics(
            spark, 
            df_limit_to_5_1, 
            "all_type_tab_iter", 
            "RpcKfXfr_RoouJ1E96_DM$$WDXR62KfuZiDVm0mE2OBA", 
            "uHKejyKFoQ-2FA1SCN3GF$$rGglAEUchvinn8LlmCFbn", 
            run_id = run_id, 
            config = subgraph_config
        )
        Lookup_1_1(spark, df_limit_to_5_1)
        df_src_dep_avro = src_dep_avro(spark)
        df_src_dep_avro = collectMetrics(
            spark, 
            df_src_dep_avro, 
            "all_type_tab_iter", 
            "gdW1R_gqqM87m2tW7Sfww$$5Z9NZrFE9DWW-zf2QGRvv", 
            "4aKCBGoEnWb5Z6yWMudT7$$9fTLii_IOGY95AyI_S0VA", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_limit_and_time_info = limit_and_time_info(spark, df_src_dep_avro)
        df_limit_and_time_info = collectMetrics(
            spark, 
            df_limit_and_time_info, 
            "all_type_tab_iter", 
            "srLpfviwgC-gGY4kGwE2u$$BYmblDxoEd-lKOiPmpqKs", 
            "JGdKqfxs7sD5OOyzWF_Ze$$Y1o0O93h6cW_AeQeXKINl", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_deduplicate_by_c_short = deduplicate_by_c_short(spark, df_limit_and_time_info)
        df_deduplicate_by_c_short = collectMetrics(
            spark, 
            df_deduplicate_by_c_short, 
            "all_type_tab_iter", 
            "ai9KMN4Arf7YzeWh9uZPA$$7yCV-WhiusBaqw0Q9LMF2", 
            "iqCLvjS-qctUHDIXDhHT1$$IRCnXjm66Bq_I1UxLKI9E", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_deduplicate_by_c_short.cache().count()
        df_deduplicate_by_c_short.unpersist()
        df_src_parquet_all_type_and_partition_withspacehyphens_renamed = src_parquet_all_type_and_partition_withspacehyphens_renamed(
            spark
        )
        df_src_parquet_all_type_and_partition_withspacehyphens_renamed = collectMetrics(
            spark, 
            df_src_parquet_all_type_and_partition_withspacehyphens_renamed, 
            "all_type_tab_iter", 
            "gigf5gRd8mPpvF9fcfxIg$$e81xy2uvxb5beWUPCsQ9O", 
            "sjSxVi-WanDLvpk2C5z9C$$NZX3OvC9bIiLUDuVyBwtd", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_limit_to_5 = limit_to_5(spark, df_src_parquet_all_type_and_partition_withspacehyphens_renamed)
        df_limit_to_5 = collectMetrics(
            spark, 
            df_limit_to_5, 
            "all_type_tab_iter", 
            "jp5cvBM1rx_RupF2o_p-J$$S6CYP-Neb-hsqWElKx2fc", 
            "dJaNvAtuz-6OPnj87Zbbe$$KXxo_SHSRPnz_aanElkCq", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_join_exclude_c_short = join_exclude_c_short(spark, out1, df_limit_to_5)
        df_join_exclude_c_short = collectMetrics(
            spark, 
            df_join_exclude_c_short, 
            "all_type_tab_iter", 
            "Xl0dEflm_4SAvG0UG2n2s$$mQFKe8i0go3hC7CfYnV2c", 
            "82MbutMmart_HV76QPTKS$$434L0cVI7ToNuNZQSnZUT", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_2_1_1 = Reformat_2_1_1(spark, df_limit_to_5)
        df_Reformat_2_1_1 = collectMetrics(
            spark, 
            df_Reformat_2_1_1, 
            "all_type_tab_iter", 
            "ZwgyOtZ1_Yxe_7U3GGV6f$$jL9FP4JTnRlCyzoe-zks3", 
            "VHjbGDC687aFjYU9Acia6$$a89L83oBvkTd59YK0Iq8K", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_5_1 = Reformat_5_1(spark, df_Reformat_2_1_1)
        df_Reformat_5_1 = collectMetrics(
            spark, 
            df_Reformat_5_1, 
            "all_type_tab_iter", 
            "akJkaxdtOJrhAfYxYzwDl$$6oBl1KABU3UJsevqMa5TV", 
            "HNJkzt9Yh998ONir9RLGD$$bf5lW6hgla2QX4EIf-Jjr", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_ComplexCategoryReformat1_1_1 = ComplexCategoryReformat1_1_1(spark, df_Reformat_5_1)
        df_ComplexCategoryReformat1_1_1 = collectMetrics(
            spark, 
            df_ComplexCategoryReformat1_1_1, 
            "all_type_tab_iter", 
            "1EGYO3RQ5yFxUuDfZEkcS$$8sGaEUdUUWzdeIYEnCiZB", 
            "IcSY3Og5jRSJ0QLEotl_1$$tdwR9kiOOO93PQ2GV-eB3", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_CustomLimit_1_2 = CustomLimit_1_2(spark, df_ComplexCategoryReformat1_1_1)
        df_CustomLimit_1_2 = collectMetrics(
            spark, 
            df_CustomLimit_1_2, 
            "all_type_tab_iter", 
            "ANV6IugeTWIxYSyzBNyzs$$MlN4tNaTAH20HMaV8CN7Z", 
            "wGZs76Ij2kw51Ge3ptSmC$$uVPJxBMJZf2_KFMGI6-Ac", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_1_1_1 = Reformat_1_1_1(spark, df_limit_to_5)
        df_Reformat_1_1_1 = collectMetrics(
            spark, 
            df_Reformat_1_1_1, 
            "all_type_tab_iter", 
            "Z03IeMyi7phLmmwDE2DkG$$S2BytKF1QApueXhgxFIrW", 
            "iMKsbZH2RyB4w9aezjzNF$$mOECB4X2wmeVJiPBK_cFX", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_join_with_multiple_tables = join_with_multiple_tables(
            spark, 
            df_Reformat_1_1_1, 
            df_Reformat_2_1_1, 
            df_join_exclude_c_short
        )
        df_join_with_multiple_tables = collectMetrics(
            spark, 
            df_join_with_multiple_tables, 
            "all_type_tab_iter", 
            "Eob7sTlWQzTmW2ZxKmn7x$$acxXZJWocSPQq1QHf-8c5", 
            "yw_1hJgF7befbTkBaRCJd$$awvO7HZbN3r8f8IfcarsD", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Limit_1_1_1 = Limit_1_1_1(spark, df_join_with_multiple_tables)
        df_Limit_1_1_1 = collectMetrics(
            spark, 
            df_Limit_1_1_1, 
            "all_type_tab_iter", 
            "z46LM1p4oMZGXVbW7gv_Y$$bLqMmQg9GP66BEgU_fn3V", 
            "zvWUKL7AZaR7XrbuidfJ3$$9PgQY5AXvH-QH5KY8w3jM", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Filter_1_1_1 = Filter_1_1_1(spark, df_Limit_1_1_1)
        df_Filter_1_1_1 = collectMetrics(
            spark, 
            df_Filter_1_1_1, 
            "all_type_tab_iter", 
            "O0eWp_RxxT4HZBkrXCKTW$$zmXehXNOj56k1irx4OiKw", 
            "Tvo1Esx4RxUdcWuy2HN3l$$8Ct6L_SygI8HfuqzTw-Ft", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_OrderBy_1_1_1 = OrderBy_1_1_1(spark, df_Filter_1_1_1)
        df_OrderBy_1_1_1 = collectMetrics(
            spark, 
            df_OrderBy_1_1_1, 
            "all_type_tab_iter", 
            "OxjD9SiJsWSj1yscwjCJ_$$xDwMRhhAne0PAo9nwbaqE", 
            "gyJ9iUYbnUog_UWtlvAy7$$YSzu-4dl2474WaVtsTQSO", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Aggregate_1_1_1 = Aggregate_1_1_1(spark, df_OrderBy_1_1_1)
        df_Aggregate_1_1_1 = collectMetrics(
            spark, 
            df_Aggregate_1_1_1, 
            "all_type_tab_iter", 
            "_HOQNy0nBJi-2aSRHQMe0$$PsrJDaTGhdDKA8H4C3JLK", 
            "tGjKapqvbf7o-N84m8S_T$$E7BKHP6SPwJSqeojMUOGa", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_SchemaTransform_1_1_1 = SchemaTransform_1_1_1(spark, df_Aggregate_1_1_1)
        df_SchemaTransform_1_1_1 = collectMetrics(
            spark, 
            df_SchemaTransform_1_1_1, 
            "all_type_tab_iter", 
            "S_h8Qh33BlAGloXAhi0vo$$ufyQ-bPrP2x3rgF02dmdz", 
            "Bs6FpvogfhY7pgbQgmHaI$$d6r6fsm4RPcM3Rhqcvxt1", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Deduplicate_1_1_1 = Deduplicate_1_1_1(spark, df_SchemaTransform_1_1_1)
        df_Deduplicate_1_1_1 = collectMetrics(
            spark, 
            df_Deduplicate_1_1_1, 
            "all_type_tab_iter", 
            "01845LnEL3Z-ESksu1ynO$$aKIhrrZwZt_Q7-mjqoMfg", 
            "dLcVZ0BA-i5KRKzn6BCrD$$4oZhpr8n7AVMVWamNi2aE", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Deduplicate_2_1_1 = Deduplicate_2_1_1(spark, df_SchemaTransform_1_1_1)
        df_Deduplicate_2_1_1 = collectMetrics(
            spark, 
            df_Deduplicate_2_1_1, 
            "all_type_tab_iter", 
            "U264yLgDWHTHMCtPfdgxB$$wCb3r0iObyJBfZRqjF6pA", 
            "6xw2Gw01Dd-2R-ipEZBvL$$uAkLU5JM0ktnKos0Z4UbC", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_SetOperation_1_1_1 = SetOperation_1_1_1(spark, df_Deduplicate_1_1_1, df_Deduplicate_2_1_1)
        df_SetOperation_1_1_1 = collectMetrics(
            spark, 
            df_SetOperation_1_1_1, 
            "all_type_tab_iter", 
            "gbd8Lwwffu_o9SH-HpPi3$$c_9jGvugLqyibmrnN85kx", 
            "NF6ZpKJ6eOoNG1wiU5mP4$$pRSpe_bvQbM9n5zpjBTjc", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_TableIterator_2 = TableIterator_2(subgraph_config.TableIterator_2, run_id).apply(spark, out0)
        df_Repartition_1_1_1 = Repartition_1_1_1(spark, df_Limit_1_1_1)
        df_Repartition_1_1_1 = collectMetrics(
            spark, 
            df_Repartition_1_1_1, 
            "all_type_tab_iter", 
            "cisTnRu6V2sHBNujMde4d$$tl9jVivRMqLN8R3WCcjog", 
            "Fqokq6FisHxsY97lrwrtN$$dU2aHar0bAGjDDWzfEwiR", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_RowDistributor_1_1_1_out0, df_RowDistributor_1_1_1_out1 = RowDistributor_1_1_1(spark, df_Repartition_1_1_1)
        df_RowDistributor_1_1_1_out0 = collectMetrics(
            spark, 
            df_RowDistributor_1_1_1_out0, 
            "all_type_tab_iter", 
            "hR7Gz4TYP0JZe0XgXFqZk$$bIX303fkEIFsOQ_7i7j6w", 
            "ZBlJMP7pvOQXGy0dWyBU4$$zqKAb0ZppHmmFVoED-q9q", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_RowDistributor_1_1_1_out1 = collectMetrics(
            spark, 
            df_RowDistributor_1_1_1_out1, 
            "all_type_tab_iter", 
            "hR7Gz4TYP0JZe0XgXFqZk$$bIX303fkEIFsOQ_7i7j6w", 
            "oC1kAHvf4ER0p6mNCzJgc$$oPVRfRGxsDaFe2XBvw603", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_WindowFunction_1_1_1 = WindowFunction_1_1_1(spark, df_SetOperation_1_1_1)
        df_WindowFunction_1_1_1 = collectMetrics(
            spark, 
            df_WindowFunction_1_1_1, 
            "all_type_tab_iter", 
            "aqWCny_ZUW4YToJ8AuHYB$$iSRgq230uCOXijP12Ojtw", 
            "YyONWhPFE7Nuk30K6OPIi$$fxA2f0slD0EC88jIYEvHG", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Script_1_1_1 = Script_1_1_1(spark, df_WindowFunction_1_1_1)
        df_Script_1_1_1 = collectMetrics(
            spark, 
            df_Script_1_1_1, 
            "all_type_tab_iter", 
            "NXAXqShwSCbEIUWVpv-Ad$$9FOAHfUccHe5U-WFWThWR", 
            "DXJhFtEgGVAHAWIRR6ve4$$x65Stt9TxlGI-Up3helmb", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Subgraph_4_1_1 = Subgraph_4_1_1(
            spark, 
            subgraph_config.Subgraph_4_1_1, 
            run_id, 
            df_RowDistributor_1_1_1_out0, 
            df_RowDistributor_1_1_1_out1, 
            df_Script_1_1_1
        )
        df_Reformat_8_1_1 = Reformat_8_1_1(spark, df_limit_to_5)
        df_Reformat_8_1_1 = collectMetrics(
            spark, 
            df_Reformat_8_1_1, 
            "all_type_tab_iter", 
            "pUqxyvWAYSu7AIs9kBOmM$$LNGV54J5xx0rciGKiyi0a", 
            "ILT2pzr42tXCWrwNeIXaN$$spKWOi2uyV63-PlWNiKHV", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Limit_3_1_1 = Limit_3_1_1(spark, df_Reformat_8_1_1)
        df_Limit_3_1_1 = collectMetrics(
            spark, 
            df_Limit_3_1_1, 
            "all_type_tab_iter", 
            "PwgfFT6lG-rozgb39e6AH$$lQjId3JWUz1zitWJCTxs_", 
            "4wE4nRGQUDsObDiWd1zHx$$ifb5DWA4HTCiRP30Tq8Fy", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Source_1_1_1 = Source_1_1_1(spark)
        df_Source_1_1_1 = collectMetrics(
            spark, 
            df_Source_1_1_1, 
            "all_type_tab_iter", 
            "8PKozLOMUrPfoWf5EytO8$$llUQvq7aaXHM8mHeig8FX", 
            "SrGa_ghZdCsV5XGdO4IqL$$xY61HCGfWZEdg70zLDhfn", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Reformat_4_2 = Reformat_4_2(spark, df_Source_1_1_1)
        df_Reformat_4_2 = collectMetrics(
            spark, 
            df_Reformat_4_2, 
            "all_type_tab_iter", 
            "rTzjgpEwPqy-NThn4nX6e$$MDkHlApkKBTJw3Kg3ELN-", 
            "CAwHusVkzECICYZ27nwmy$$4fLfHnQsXB1hltD0fMNKR", 
            run_id = run_id, 
            config = subgraph_config
        )
        py_sg_target_test_release(spark, df_Reformat_4_2)
        df_CustomReformatGem_1_3 = CustomReformatGem_1_3(spark, df_CustomLimit_1_2)
        df_CustomReformatGem_1_3 = collectMetrics(
            spark, 
            df_CustomReformatGem_1_3, 
            "all_type_tab_iter", 
            "LAs5_gZwdxOJwPnnk_63f$$rx2yY6qp7M7uvGgQA_QkU", 
            "X8_3Qu6sYavvMaYU_VwJR$$dIchBnuIRAKukh_Gd595b", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_CustomReformatGem_1_3.cache().count()
        df_CustomReformatGem_1_3.unpersist()
        df_OrderBy_3_1_1 = OrderBy_3_1_1(spark, df_limit_to_5)
        df_OrderBy_3_1_1 = collectMetrics(
            spark, 
            df_OrderBy_3_1_1, 
            "all_type_tab_iter", 
            "G8g6FrOVofVKxrjdIOl9p$$lAWPx_giZgdF9RPd0T_7e", 
            "t1CzUaLbTpyEDrRt15tsq$$bsJ5gtR72gtvaEcwJJPws", 
            run_id = run_id, 
            config = subgraph_config
        )
        df_Deduplicate_3_1_1 = Deduplicate_3_1_1(spark, df_OrderBy_3_1_1)
        df_Deduplicate_3_1_1 = collectMetrics(
            spark, 
            df_Deduplicate_3_1_1, 
            "all_type_tab_iter", 
            "OKZU50EMhQH0AjqtrxvgU$$XhtlibXQ8YahlhoebLoia", 
            "LsaE_krHa-yRdvnhnbyNN$$yVRfdXqtF4OHrkzXYMmcE", 
            run_id = run_id, 
            config = subgraph_config
        )
        subgraph_config.update(Config)

        return list((df_TableIterator_2, df_Limit_3_1_1, df_Subgraph_4_1_1, df_Deduplicate_3_1_1))

    def apply(
            self, 
            spark: SparkSession,
            in0: DataFrame,
            in1: DataFrame, 
            in2: DataFrame
    ) -> (DataFrame, DataFrame, DataFrame, DataFrame):
        inDFs = [in1, in2]
        conf_to_column = dict(
            [("c_short", "c_short"),  ("c_int", "c_int"),  ("c_long", "c_long"),  ("c_float", "c_float"),              ("c_boolean", "c_boolean"),  ("c_double", "c_double"),  ("c_string", "c_string"),              ("c_date", "c_date"),  ("c_timestamp", "c_timestamp"),  ("c_array_int", "c_array_int"),              ("c_array_string", "c_array_string"),  ("c_array_long", "c_array_long"),              ("c_array_boolean", "c_array_boolean"),  ("c_array_date", "c_array_date"),              ("c_array_timestamp", "c_array_timestamp"),  ("c_array_float", "c_array_float"),              ("c_array_decimal", "c_array_decimal"),  ("c_struct", "c_struct")]
        )

        if in0.count() > 1000:
            raise Exception(f"Config DataFrame row count::{in0.count()} exceeds max run count")

        import multiprocessing
        from multiprocessing.pool import ThreadPool
        from functools import partial

        with ThreadPool(processes = 5) as pool:

            def process_row(row, config, inDFs, spark):
                df1 = config.update_from_row_map(row, conf_to_column)

                return self.__run__(spark, df1, *inDFs)

            partial_process_row = partial(process_row, config = self.config, inDFs = [in1, in2], spark = spark)
            results = pool.map(partial_process_row, in0.collect())

            return do_union(results)
