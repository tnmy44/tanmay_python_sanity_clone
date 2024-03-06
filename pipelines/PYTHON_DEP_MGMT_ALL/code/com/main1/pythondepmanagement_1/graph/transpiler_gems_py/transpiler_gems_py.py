from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def transpiler_gems_py(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_csv_basic = csv_basic(spark)
    df_csv_basic = collectMetrics(
        spark, 
        df_csv_basic, 
        "transpiler_gems_py", 
        "t6I6Jx6oTdVUu5nUKbALT$$9bYrXXI0O-LnHEWO9RbAF", 
        "x4e-UR_mFK8KJwh-GmBHu$$mbsmi0nlY5HT6vyBmqkeY"
    )
    df_reformat_columns = reformat_columns(spark, df_csv_basic)
    df_reformat_columns = collectMetrics(
        spark, 
        df_reformat_columns, 
        "transpiler_gems_py", 
        "jEikVl3hyvLnxG8OZyUuv$$rlxzpiPZOELlKKZuGhr-_", 
        "fV8p3ijfXEXc58q0oqGBO$$lvzkxyKNoCaTnWsjScjFp"
    )
    df_CompareRecords_1 = CompareRecords_1(spark, df_csv_basic, df_csv_basic)
    df_CompareRecords_1 = collectMetrics(
        spark, 
        df_CompareRecords_1, 
        "transpiler_gems_py", 
        "SIiyrUJKZ7jcFe9eQmgxM$$dlBzW--vzo7qjf6MsbG-3", 
        "cQJxPdFSafoLhCpiT0Kz_$$iK6-RanoCiguS32nifHBn"
    )
    df_CompareRecords_1.cache().count()
    df_CompareRecords_1.unpersist()
    df_sync_dataframe_columns_with_schema = sync_dataframe_columns_with_schema(spark, df_csv_basic)
    df_sync_dataframe_columns_with_schema = collectMetrics(
        spark, 
        df_sync_dataframe_columns_with_schema, 
        "transpiler_gems_py", 
        "fp5J6EPrJ_nr2fuVdigH-$$eaWh-6R5v-8yarzEB9LYJ", 
        "71A5TmFz_H9hj-jaKfiTX$$GmEn6mQb58VvfYozlfy1v"
    )
    df_sync_dataframe_columns_with_schema.cache().count()
    df_sync_dataframe_columns_with_schema.unpersist()
    df_add_index_column = add_index_column(spark, df_csv_basic)
    df_add_index_column = collectMetrics(
        spark, 
        df_add_index_column, 
        "transpiler_gems_py", 
        "89kuyOn46_7PW20Pz3Fug$$0b3XXBcQ6FlO77pCSasPc", 
        "v440CRIQJq-a7U7wqwKUE$$5YEbU1cndHO9qflAJftqy"
    )
    df_add_index_column.cache().count()
    df_add_index_column.unpersist()
    df_RoundRobinPartition_1_out0, df_RoundRobinPartition_1_out1 = RoundRobinPartition_1(spark, df_csv_basic)
    df_RoundRobinPartition_1_out0 = collectMetrics(
        spark, 
        df_RoundRobinPartition_1_out0, 
        "transpiler_gems_py", 
        "RVNag7KpqgO9VWPPJg1Ce$$f-lzunZJrVcNdN96FVCN6", 
        "PHUfKtnE7Bz8ug8rKAPUa$$_1SF49mk9f4vqxNh7hxy6"
    )
    df_RoundRobinPartition_1_out1 = collectMetrics(
        spark, 
        df_RoundRobinPartition_1_out1, 
        "transpiler_gems_py", 
        "RVNag7KpqgO9VWPPJg1Ce$$f-lzunZJrVcNdN96FVCN6", 
        "ooHam7r1-if8VMWd3NeCS$$NvT_dWPH-TxvkZEDnmKMd"
    )
    df_RoundRobinPartition_1_out0.cache().count()
    df_RoundRobinPartition_1_out0.unpersist()
    df_RoundRobinPartition_1_out1.cache().count()
    df_RoundRobinPartition_1_out1.unpersist()
    df_write_csv_with_colon_separator = write_csv_with_colon_separator(spark, df_csv_basic)
    df_write_csv_with_colon_separator = collectMetrics(
        spark, 
        df_write_csv_with_colon_separator, 
        "transpiler_gems_py", 
        "XD8FnUNrjHanZ0OYWY0y-$$osiZD8XejHjNGg7KE1k_Q", 
        "bzwsTGcTGOIL0ri62axs4$$g5GfzrmuytYBG-_GKjXFu"
    )
    df_write_csv_with_colon_separator.cache().count()
    df_write_csv_with_colon_separator.unpersist()
    df_meta_pivot_by_column = meta_pivot_by_column(spark, df_csv_basic)
    df_meta_pivot_by_column = collectMetrics(
        spark, 
        df_meta_pivot_by_column, 
        "transpiler_gems_py", 
        "xgMx4eX9A5605YiDAeXM0$$kwf1NFRl-jVDzfjFUXS9c", 
        "278xr3MJNg_9XfYSdf_6D$$0ll6DbGdi0w4ng8IT2z2x"
    )
    df_meta_pivot_by_column.cache().count()
    df_meta_pivot_by_column.unpersist()
    df_read_separated_values = read_separated_values(spark, df_csv_basic)
    df_read_separated_values = collectMetrics(
        spark, 
        df_read_separated_values, 
        "transpiler_gems_py", 
        "yUQnTxIFJ2OD-ejbZqOwN$$PZWFAoora8IuxknERhHNS", 
        "PdNXQFaDNAgbbVycOk1oQ$$0PD-p634bgHZSegQuMaNn"
    )
    df_read_separated_values.cache().count()
    df_read_separated_values.unpersist()
    df_generate_dataframe_log = generate_dataframe_log(spark, df_csv_basic)
    df_generate_dataframe_log = collectMetrics(
        spark, 
        df_generate_dataframe_log, 
        "transpiler_gems_py", 
        "oLAWs_s5czJgAcKT8kuse$$PF4kxn0x8EInGQbqLYTpk", 
        "R0BSI_Z6PNbdBlomrO_l4$$Y05zX6I9oOCQSW9T9Wbty"
    )
    df_generate_dataframe_log.cache().count()
    df_generate_dataframe_log.unpersist()
    df_assertion_check = assertion_check(spark, df_csv_basic)
    df_assertion_check = collectMetrics(
        spark, 
        df_assertion_check, 
        "transpiler_gems_py", 
        "34pamk6BOsg07k6PJfp6k$$qFCp3MBeEpkLJ4ySF4CN2", 
        "rpjrmQDRY1VBwtK6y57Si$$0X5Va1-Q9f82I4-UVr-Is"
    )
    df_assertion_check.cache().count()
    df_assertion_check.unpersist()
    df_generate_surrogate_keys_out0, df_generate_surrogate_keys_out1, df_generate_surrogate_keys_out2 = generate_surrogate_keys(
        spark, 
        df_reformat_columns, 
        df_reformat_columns
    )
    df_generate_surrogate_keys_out0 = collectMetrics(
        spark, 
        df_generate_surrogate_keys_out0, 
        "transpiler_gems_py", 
        "6gifUeBdh-fGEMGLMY6f4$$IQfT3SHuVYDqzr6cCPukz", 
        "9xMmFHSv7WB6fbDZCKqJh$$F43eNJOxr2r1BV9KdNvIv"
    )
    df_generate_surrogate_keys_out1 = collectMetrics(
        spark, 
        df_generate_surrogate_keys_out1, 
        "transpiler_gems_py", 
        "6gifUeBdh-fGEMGLMY6f4$$IQfT3SHuVYDqzr6cCPukz", 
        "gGNago9-f6Lyt_ib4iYHU$$R9CXSf5CPVFDQmgsYTahK"
    )
    df_generate_surrogate_keys_out2 = collectMetrics(
        spark, 
        df_generate_surrogate_keys_out2, 
        "transpiler_gems_py", 
        "6gifUeBdh-fGEMGLMY6f4$$IQfT3SHuVYDqzr6cCPukz", 
        "3na8IpFk5JLMhMk5RSp3Q$$ocRlM-kHPfYSy-mYfShzc"
    )
    df_generate_surrogate_keys_out0.cache().count()
    df_generate_surrogate_keys_out0.unpersist()
    df_generate_surrogate_keys_out1.cache().count()
    df_generate_surrogate_keys_out1.unpersist()
    df_generate_surrogate_keys_out2.cache().count()
    df_generate_surrogate_keys_out2.unpersist()
    df_normalize_data = normalize_data(spark, df_csv_basic)
    df_normalize_data = collectMetrics(
        spark, 
        df_normalize_data, 
        "transpiler_gems_py", 
        "VN_IH7MxvpCwXW-UL8Tzm$$i9TN0XxIvrw3HkXkVu1B1", 
        "VKZCyzgERgqJH5NvScvsm$$IZE1AWXAy6lL-jQXBzvtc"
    )
    df_normalize_data.cache().count()
    df_normalize_data.unpersist()
    subgraph_config.update(Config)
