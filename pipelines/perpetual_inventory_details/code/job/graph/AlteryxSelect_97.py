from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_97(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Facility"), 
        col("PartNumber"), 
        col("TransactedBy"), 
        col("Plant"), 
        col("DateTransacted"), 
        col("Note"), 
        col("ClosingBalance"), 
        col("TransactionType"), 
        col("TransactionId"), 
        col("Quantity"), 
        col("RollingBalance"), 
        col("OpeningBalance")
    )
