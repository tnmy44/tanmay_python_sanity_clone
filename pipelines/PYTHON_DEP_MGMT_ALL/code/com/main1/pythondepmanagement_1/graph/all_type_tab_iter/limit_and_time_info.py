from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def limit_and_time_info(spark: SparkSession, in0: DataFrame) -> DataFrame:
    import pendulum
    print(pendulum.now('Europe/Paris'))
    print(pendulum.now().subtract(minutes = 2).diff_for_humans())
    from gensim.models import Word2Vec

    return in0.limit(5)
