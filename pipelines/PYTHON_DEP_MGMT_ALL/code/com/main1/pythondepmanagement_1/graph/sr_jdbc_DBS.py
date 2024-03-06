from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def sr_jdbc_DBS(spark: SparkSession) -> DataFrame:
    import os
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.JDBC_URL}")\
        .option("user", f"{Config.JDBC_USERNAME}")\
        .option("password", f"{Config.JDBC_PASSWORD}")\
        .option("dbtable", "test_table")\
        .option("pushDownPredicate", True)\
        .option("driver", "com.mysql.jdbc.Driver")\
        .load()
