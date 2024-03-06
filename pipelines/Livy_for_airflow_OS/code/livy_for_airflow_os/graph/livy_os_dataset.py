from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from livy_for_airflow_os.config.ConfigStore import *
from livy_for_airflow_os.udfs.UDFs import *

def livy_os_dataset(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Sector", StringType(), True), StructField("Year", StringType(), True), StructField("Month", StringType(), True), StructField("Cereal and Products", StringType(), True), StructField("Pulses and Pulse Products", StringType(), True), StructField("Oils and Fats", StringType(), True), StructField("Meat - Fish etc", StringType(), True), StructField("Milk and Milk Products", StringType(), True), StructField("Spices and Condiments", StringType(), True), StructField("Vegetables", StringType(), True), StructField("Fruits", StringType(), True), StructField("Sugar - Honey etc", StringType(), True), StructField("Non Alcoholic Beverages", StringType(), True), StructField("Prepared Meals", StringType(), True), StructField("Pan - Supari etc", StringType(), True), StructField("Food - beverages and tobacco ", StringType(), True), StructField("Fuel and Lighting", StringType(), True), StructField("Housing", StringType(), True), StructField("Clothing and Bedding", StringType(), True), StructField("Footwear", StringType(), True), StructField("Clothing - bedding and footwear", StringType(), True), StructField("Medical Care", StringType(), True), StructField("Education", StringType(), True), StructField("Recreation and Amusement", StringType(), True), StructField("Transport and Communication", StringType(), True), StructField("Personal Care Items", StringType(), True), StructField("Household Requisites", StringType(), True), StructField("Others", StringType(), True), StructField("Miscellaneous", StringType(), True), StructField("General index", StringType(), True), StructField("_c30", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("hdfs://ip-10-0-0-91.us-west-2.compute.internal:8020/user/hadoop/data/csv/All_India_Index-February2016.csv")
