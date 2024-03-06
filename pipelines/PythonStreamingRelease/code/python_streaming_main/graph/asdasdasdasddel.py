from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_streaming_main.config.ConfigStore import *
from python_streaming_main.udfs.UDFs import *

def asdasdasdasddel(spark: SparkSession) -> DataFrame:
    consumer_options = {
        "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"asdsad\" password=\"asdas\";",
        "kafka.sasl.mechanism": "SCRAM-SHA-256",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.bootstrap.servers": "sadsad",
        "kafka.session.timeout.ms": "6000",
        "group.id": "",
    }
    consumer_options["subscribe"] = "asdsad"
    consumer_options["startingOffsets"] = "latest"
    consumer_options["includeHeaders"] = True
    from pyspark.sql.avro.functions import from_avro

    return (spark.readStream\
        .format("kafka")\
        .options(**consumer_options)\
        .load()\
        .withColumn("value", col("value").cast("string"))\
        .withColumn("key", col("key").cast("string")))\
        .withColumn("value", from_avro(
        col("value").cast(BinaryType()),
        """{
  \"type\": \"record\",
  \"name\": \"Record\",
  \"fields\": [
    {
      \"name\": \"order_id\",
      \"type\": \"long\"
    },
    {
      \"name\": \"customer_id\",
      \"type\": \"long\"
    },
    {
      \"name\": \"order_category\",
      \"type\": \"string\"
    },
    {
      \"name\": \"order_date\",
      \"type\": \"string\"
    },
    {
      \"name\": \"amount\",
      \"type\": \"double\"
    }
  ]
}"""
    ))
