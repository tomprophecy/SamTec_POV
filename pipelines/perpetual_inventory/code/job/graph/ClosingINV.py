from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ClosingINV(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PartNumber"), 
        col("UnitCost").alias("ClosingUnitCost"), 
        col("LaborCost").alias("ClosingLaborCost"), 
        col("PlatingCost").alias("ClostingPlatingCost"), 
        col("PlannerCode").alias("ClosingPlannerCode"), 
        col("Buyer").alias("ClosingBuyer"), 
        col("OrderPolicy").alias("ClosingOrderPolicy"), 
        col("CustomerSupplied").alias("ClosingCustomerSupplied"), 
        col("StandardCost").alias("ClosingStandardUnitCost"), 
        col("StandardLaborCost").alias("ClosingStandardLaborCost"), 
        col("StandardPlatingCost").alias("ClosingStandardPlatingCost")
    )
