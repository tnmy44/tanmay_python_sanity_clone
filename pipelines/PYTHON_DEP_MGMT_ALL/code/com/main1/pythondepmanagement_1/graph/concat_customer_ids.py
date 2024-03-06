from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def concat_customer_ids(spark: SparkSession, in0: DataFrame, in1: DataFrame):
    df1 = in1.select("customer_id", "first_name", "last_name")
    df2 = in0.select("customer_id", "first_name", "last_name")
    print(df1.show())
    print(df2.show())
    concat_df = df1.union(df2)
    print(concat_df.show())
    schema = concat_df.printSchema()
    print(schema)

    return 
