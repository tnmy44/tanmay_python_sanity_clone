from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from livy_for_airflow_os.config.ConfigStore import *
from livy_for_airflow_os.udfs.UDFs import *

def print_sector(spark: SparkSession, in0: DataFrame) -> DataFrame:
    print(col("Sector"))
    out0 = in0

    return out0
