from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CostsCodes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PreviousQuarterStandardUnitCost"), 
        col("ClosingPlannerCode"), 
        col("OpeningIsBOM"), 
        col("ClostingPlatingCost"), 
        col("PreviousQuarterIsBOM"), 
        col("PreviousQuarterLaborCost"), 
        col("ClosingStandardLaborCost"), 
        col("PreviousQuarterUnitCost"), 
        col("OpeningStandardLaborCost"), 
        col("PreviousQuarterPlannerCode"), 
        col("PartNumber"), 
        col("ClosingLaborCost"), 
        col("ClosingCustomerSupplied"), 
        col("OpeningStandardPlatingCost"), 
        col("OpeningCustomerSupplied"), 
        col("ClosingStandardUnitCost"), 
        col("PreviousQuarterOrderPolicy"), 
        col("OpeningPlannerCode"), 
        col("ClosingBuyer"), 
        col("PreviousQuarterStandardLaborCost"), 
        col("ClosingStandardPlatingCost"), 
        col("OpeningBuyer"), 
        col("OpeningOrderPolicy"), 
        col("PreviousQuarterBuyer"), 
        col("PreviousQuarterCustomerSupplied"), 
        col("OpeningLaborCost"), 
        col("ClosingUnitCost"), 
        col("ClosingOrderPolicy"), 
        col("PreviousQuarterStandardPlatingCost"), 
        col("OpeningUnitCost"), 
        col("ClosingIsBOM"), 
        col("OpeningPlatingCost"), 
        col("PreviousQuarterPlatingCost"), 
        col("OpeningStandardUnitCost")
    )
