from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_182(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("part_number").alias("PartNumber"), 
        col("reporting_series").alias("ReportingSeries"), 
        col("reporting_series_era").alias("ReportingSeriesProductEra"), 
        col("reporting_series_weave").alias("ReportingSeriesProductWeave"), 
        col("reporting_series_block").alias("ReportingSeriesProductSolutionBlock"), 
        col("reporting_series_segment").alias("ReportingSeriesProductSegment"), 
        col("dm_item_type").alias("ItemType"), 
        col("pcmfg_item_type").alias("PCMfgItemType")
    )
