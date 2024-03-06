from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from com.main1.pythondepmanagement_1.config.ConfigStore import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def deduplicate_by_c_short(spark: SparkSession, in0: DataFrame) -> DataFrame:
    import pendulum
    from gensim.models import Word2Vec
    print(pendulum.now('Europe/Paris'))
    print(pendulum.now().subtract(minutes = 2).diff_for_humans())

    return in0.dropDuplicates(["first_name"])
