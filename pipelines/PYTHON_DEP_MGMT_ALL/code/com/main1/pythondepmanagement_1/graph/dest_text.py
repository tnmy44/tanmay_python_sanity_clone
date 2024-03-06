from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def dest_text(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("text")\
        .mode("overwrite")\
        .text("dbfs:/tmp/e2e/sanity/python/textdest1", compression = None, lineSep = None)
