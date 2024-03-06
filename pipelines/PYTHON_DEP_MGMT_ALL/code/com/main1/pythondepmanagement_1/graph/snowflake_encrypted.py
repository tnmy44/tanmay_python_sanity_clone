from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def snowflake_encrypted(spark: SparkSession) -> DataFrame:
    import re
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://tu22760.ap-south-1.aws.snowflakecomputing.com",
            "sfUser": "AUTOMATIONTESTUSER",
            "sfDatabase": "QA_DATABASE",
            "sfSchema": "qa_schema",
            "sfWarehouse": "QA_Warehouse",
            "pem_private_key": re\
              .sub(
                "-*(BEGIN|END) PRIVATE KEY-*\n",
                "",
                serialization\
                  .load_pem_private_key(
                    spark.sparkContext.wholeTextFiles("dbfs:/FileStore/rsa_key-1.p8").first()[1].encode(),
                    password = None,
                    backend = default_backend()
                  )\
                  .private_bytes(
                    encoding = serialization.Encoding.PEM,
                    format = serialization.PrivateFormat("PKCS8"),
                    encryption_algorithm = serialization.NoEncryption()
                  )\
                  .decode("UTF-8")
              )\
              .replace("\n", ""),
            "sfRole": "QA_ROLE"
          }
        )\
        .option("dbtable", "EMPLOYEES")\
        .load()
