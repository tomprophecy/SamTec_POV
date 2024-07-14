from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_76(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Base Transaction Type`").alias("Base Transaction Type"), 
        col("`Accounting Transaction Type`").alias("Accounting Transaction Type")
    )
