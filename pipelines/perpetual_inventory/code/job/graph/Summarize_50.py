from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_50(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg(min(col("FileDate")).alias("FirstDate"), max(col("FileDate")).alias("LastDate"))
