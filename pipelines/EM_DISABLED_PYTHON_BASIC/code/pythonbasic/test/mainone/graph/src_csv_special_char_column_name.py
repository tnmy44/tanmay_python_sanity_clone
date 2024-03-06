from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *

def src_csv_special_char_column_name(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("c   short  --", ShortType(), True), StructField("c-int-column type", StringType(), True), StructField("-- c-long", StringType(), True), StructField("c-decimal", StringType(), True), StructField("c  float", StringType(), True), StructField("c--boolean", BooleanType(), True), StructField("c- - -double", StringType(), True), StructField("c___-- string", StringType(), True), StructField("c  date", StringType(), True), StructField("c_timestamp", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/qa_data/csv/special_char_column_name")
