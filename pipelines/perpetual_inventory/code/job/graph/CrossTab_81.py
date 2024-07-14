from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_81(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("PartNumber"), col("Facility"), col("Plant"))
    df2 = df1.pivot("TransactionType")

    return df2.agg(sum(col("Quantity")).alias("Quantity"))
