from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pythonbasic.test.mainone.config.ConfigStore import *
from pythonbasic.test.mainone.udfs.UDFs import *

def dest_testpythonsanitybasic(spark: SparkSession, in0: DataFrame):
    in0.write.format("parquet").mode("overwrite").save("dbfs:/tmp/e2e/dest_testpythonsanitybasic/test123")
