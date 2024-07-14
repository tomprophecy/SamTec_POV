from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_118(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("PartNumber"), 
        col("OpeningUnitCost"), 
        col("ClosingUnitCost"), 
        col("PreviousQuarterUnitCost"), 
        col("OpeningLaborCost"), 
        col("ClosingLaborCost"), 
        col("PreviousQuarterLaborCost"), 
        col("OpeningPlatingCost"), 
        col("ClostingPlatingCost"), 
        col("PreviousQuarterPlatingCost"), 
        col("OpeningPlannerCode"), 
        col("ClosingPlannerCode"), 
        col("PreviousQuarterPlannerCode"), 
        col("OpeningIsBOM"), 
        col("ClosingIsBOM"), 
        col("PreviousQuarterIsBOM"), 
        col("OpeningBuyer"), 
        col("ClosingBuyer"), 
        col("PreviousQuarterBuyer"), 
        col("OpeningOrderPolicy"), 
        col("ClosingOrderPolicy"), 
        col("PreviousQuarterOrderPolicy"), 
        col("OpeningCustomerSupplied"), 
        col("ClosingCustomerSupplied"), 
        col("PreviousQuarterCustomerSupplied"), 
        col("OpeningStandardUnitCost"), 
        col("OpeningStandardLaborCost"), 
        col("OpeningStandardPlatingCost"), 
        col("ClosingStandardUnitCost"), 
        col("ClosingStandardLaborCost"), 
        col("ClosingStandardPlatingCost"), 
        col("PreviousQuarterStandardUnitCost"), 
        col("PreviousQuarterStandardLaborCost"), 
        col("PreviousQuarterStandardPlatingCost")
    )
