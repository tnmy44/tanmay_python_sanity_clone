from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def dest_custom_avro(spark: SparkSession, in0: DataFrame):
    in0.write.format("avro").mode("overwrite").save("dbfs:/tmp/e2e/sanity/python/customavrodest3")
