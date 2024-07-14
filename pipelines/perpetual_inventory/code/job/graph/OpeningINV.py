from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def OpeningINV(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PartNumber"), 
        col("UnitCost").alias("OpeningUnitCost"), 
        col("LaborCost").alias("OpeningLaborCost"), 
        col("PlatingCost").alias("OpeningPlatingCost"), 
        col("PlannerCode").alias("OpeningPlannerCode"), 
        col("Buyer").alias("OpeningBuyer"), 
        col("OrderPolicy").alias("OpeningOrderPolicy"), 
        col("CustomerSupplied").alias("OpeningCustomerSupplied"), 
        col("StandardCost").alias("OpeningStandardUnitCost"), 
        col("StandardLaborCost").alias("OpeningStandardLaborCost"), 
        col("StandardPlatingCost").alias("OpeningStandardPlatingCost")
    )
