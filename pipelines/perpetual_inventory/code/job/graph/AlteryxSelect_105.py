from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_105(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("StandardPlatingCost"), 
        col("PlannerCode"), 
        col("LaborCost"), 
        col("Buyer"), 
        col("PartNumber"), 
        col("OrderPolicy"), 
        col("StandardLaborCost"), 
        col("PlatingCost"), 
        col("StandardCost"), 
        col("UnitCost"), 
        col("CustomerSupplied")
    )
