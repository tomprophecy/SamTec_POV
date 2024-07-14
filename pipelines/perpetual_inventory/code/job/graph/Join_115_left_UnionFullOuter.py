from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_115_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.PartNumber") == col("in1.PartNumber")), "fullouter")\
        .select(col("in1.PreviousQuarterPlatingCost").alias("PreviousQuarterPlatingCost"), col("in0.ClosingStandardUnitCost").alias("ClosingStandardUnitCost"), col("in1.PreviousQuarterCustomerSupplied").alias("PreviousQuarterCustomerSupplied"), col("in0.ClosingStandardLaborCost").alias("ClosingStandardLaborCost"), col("in1.PreviousQuarterLaborCost").alias("PreviousQuarterLaborCost"), col("in0.ClosingStandardPlatingCost").alias("ClosingStandardPlatingCost"), col("in0.OpeningStandardUnitCost").alias("OpeningStandardUnitCost"), col("in1.PreviousQuarterPlannerCode").alias("PreviousQuarterPlannerCode"), col("in0.ClosingPlannerCode").alias("ClosingPlannerCode"), col("in1.PreviousQuarterBuyer").alias("PreviousQuarterBuyer"), col("in0.OpeningBuyer").alias("OpeningBuyer"), col("in1.PreviousQuarterUnitCost").alias("PreviousQuarterUnitCost"), col("in0.OpeningPlatingCost").alias("OpeningPlatingCost"), col("in0.ClosingLaborCost").alias("ClosingLaborCost"), col("in0.OpeningOrderPolicy").alias("OpeningOrderPolicy"), col("in0.OpeningUnitCost").alias("OpeningUnitCost"), col("in0.ClosingBuyer").alias("ClosingBuyer"), col("in0.PartNumber").alias("PartNumber"), col("in1.PreviousQuarterStandardLaborCost").alias("PreviousQuarterStandardLaborCost"), col("in0.OpeningPlannerCode").alias("OpeningPlannerCode"), col("in0.ClosingUnitCost").alias("ClosingUnitCost"), col("in0.ClosingOrderPolicy").alias("ClosingOrderPolicy"), col("in0.OpeningStandardLaborCost").alias("OpeningStandardLaborCost"), col("in0.OpeningLaborCost").alias("OpeningLaborCost"), col("in0.ClosingCustomerSupplied").alias("ClosingCustomerSupplied"), col("in1.PreviousQuarterOrderPolicy").alias("PreviousQuarterOrderPolicy"), col("in0.ClostingPlatingCost").alias("ClostingPlatingCost"), col("in0.OpeningCustomerSupplied").alias("OpeningCustomerSupplied"), col("in1.PreviousQuarterStandardUnitCost").alias("PreviousQuarterStandardUnitCost"), col("in1.PreviousQuarterStandardPlatingCost").alias("PreviousQuarterStandardPlatingCost"), col("in0.OpeningStandardPlatingCost").alias("OpeningStandardPlatingCost"))
