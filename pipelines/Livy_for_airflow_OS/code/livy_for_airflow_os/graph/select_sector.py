from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from livy_for_airflow_os.config.ConfigStore import *
from livy_for_airflow_os.udfs.UDFs import *

def select_sector(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(lit(Config.c1.c2.c3_arr[0].c4[0].stry1).alias("Sector"))
