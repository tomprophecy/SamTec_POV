from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_83_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Produced").cast(IntegerType()).alias("Produced"), 
        col("PartNumber"), 
        col("TransferredOut").cast(IntegerType()).alias("TransferredOut"), 
        col("Adjusted").cast(IntegerType()).alias("Adjusted"), 
        col("Facility"), 
        col("Plant"), 
        col("TransferredIn").cast(IntegerType()).alias("TransferredIn"), 
        col("Purchased").cast(IntegerType()).alias("Purchased"), 
        col("Sold").cast(IntegerType()).alias("Sold"), 
        col("Issued").cast(IntegerType()).alias("Issued")
    )
