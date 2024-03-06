from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def catalog_with_filter_predicate(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f'SELECT * FROM `qa_database`.`all_type_parquet` WHERE c_string = CONCAT_WS("b", "a", \'{Config.raw_schema}\')'
    )
