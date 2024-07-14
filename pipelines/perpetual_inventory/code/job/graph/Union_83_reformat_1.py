from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_83_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Facility"), 
        col("Plant"), 
        col("PartNumber"), 
        lit(None).cast(IntegerType()).alias("Produced"), 
        lit(None).cast(IntegerType()).alias("TransferredOut"), 
        lit(None).cast(IntegerType()).alias("Adjusted"), 
        lit(None).cast(IntegerType()).alias("TransferredIn"), 
        lit(None).cast(IntegerType()).alias("Purchased"), 
        lit(None).cast(IntegerType()).alias("Sold"), 
        lit(None).cast(IntegerType()).alias("Issued")
    )
