from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DynamicInput_64_fileName(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "fileName",
        concat(
          lit("X:\\adw\\operations\\material management\\material reconciliation\\transactions\\2016-06-01.yxdb"), 
          col("FileDate")
        )
    )
