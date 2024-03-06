from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def DepReformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    import pendulum
    from gensim.models import Word2Vec
    print(pendulum.now('Europe/Paris'))
    print(pendulum.now().subtract(minutes = 2).diff_for_humans())

    return in0
