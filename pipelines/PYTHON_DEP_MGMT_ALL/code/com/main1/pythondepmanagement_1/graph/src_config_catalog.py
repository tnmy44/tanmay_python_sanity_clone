from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def src_config_catalog(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`hive_metastore`.`{Config.CATALOG_DATABASE}`.`{Config.CATALOG_TABLENAME}`")
