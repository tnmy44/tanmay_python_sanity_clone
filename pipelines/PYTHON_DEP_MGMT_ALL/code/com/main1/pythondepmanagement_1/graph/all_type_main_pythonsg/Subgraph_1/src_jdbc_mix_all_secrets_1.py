from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def src_jdbc_mix_all_secrets_1(spark: SparkSession) -> DataFrame:
    import os
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("jdbc")\
        .option(
          "url",
          "jdbc:mysql://{}:{}/{}".format(
            DBUtils(spark).secrets.get(scope = "qasecrets", key = "mysql_host"), 
            DBUtils(spark).secrets.get(scope = "qasecrets", key = "mysql_port"), 
            Config.JDBC_DATABASE
          )
        )\
        .option("user", "{}".format(SecretManager(spark).get("secrets/qa", "mysql_username", "HashiCorp")))\
        .option("password", "{}".format(DBUtils(spark).secrets.get(scope = "qasecrets", key = "mysql_password")))\
        .option("query", "select * from test_table_automation")\
        .option("pushDownPredicate", True)\
        .option("driver", "com.mysql.jdbc.Driver")\
        .load()
