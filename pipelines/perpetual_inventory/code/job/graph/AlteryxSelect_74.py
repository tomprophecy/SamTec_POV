from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_74(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("ReportingSeries").alias("Series"), 
        col("PCMfgItemType"), 
        col("ReportingSeriesProductSegment").alias("Segment"), 
        col("ReportingSeriesProductSolutionBlock").alias("Block"), 
        col("PartNumber"), 
        col("ReportingSeriesProductWeave").alias("Weave"), 
        col("ReportingSeriesProductEra").alias("Era"), 
        col("ItemType")
    )
