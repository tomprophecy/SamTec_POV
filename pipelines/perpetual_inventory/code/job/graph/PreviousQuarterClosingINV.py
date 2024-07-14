from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PreviousQuarterClosingINV(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PartNumber"), 
        col("UnitCost").alias("PreviousQuarterUnitCost"), 
        col("LaborCost").alias("PreviousQuarterLaborCost"), 
        col("PlatingCost").alias("PreviousQuarterPlatingCost"), 
        col("PlannerCode").alias("PreviousQuarterPlannerCode"), 
        col("Buyer").alias("PreviousQuarterBuyer"), 
        col("OrderPolicy").alias("PreviousQuarterOrderPolicy"), 
        col("CustomerSupplied").alias("PreviousQuarterCustomerSupplied"), 
        col("StandardCost").alias("PreviousQuarterStandardUnitCost"), 
        col("StandardLaborCost").alias("PreviousQuarterStandardLaborCost"), 
        col("StandardPlatingCost").alias("PreviousQuarterStandardPlatingCost")
    )
