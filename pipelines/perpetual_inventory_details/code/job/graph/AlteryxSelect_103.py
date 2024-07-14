from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_103(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Facility"), 
        col("Plant"), 
        col("PartNumber"), 
        col("OpeningBalance"), 
        col("ClosingBalance"), 
        col("TransactionId"), 
        col("TransactionType").alias("SystemType"), 
        col("`Accounting Transaction Type`").alias("AccountingType"), 
        col("Quantity"), 
        col("RollingBalance"), 
        col("DateTransacted"), 
        col("TransactedBy"), 
        col("Note")
    )
