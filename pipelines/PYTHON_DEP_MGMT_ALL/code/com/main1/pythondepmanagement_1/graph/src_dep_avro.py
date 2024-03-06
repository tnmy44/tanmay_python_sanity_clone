from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def src_dep_avro(spark: SparkSession) -> DataFrame:
    import pendulum
    from gensim.models import Word2Vec
    print(pendulum.now('Europe/Paris'))
    print(pendulum.now().subtract(minutes = 2).diff_for_humans())

    return spark.read.format("avro").load("dbfs:/Prophecy/qa_data/avro/CustomersDatasetInput.avro")
