from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_114_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.PartNumber") == col("in1.PartNumber")), "fullouter")\
        .select(col("in1.ClosingStandardUnitCost").alias("ClosingStandardUnitCost"), col("in1.ClosingStandardLaborCost").alias("ClosingStandardLaborCost"), col("in1.ClosingStandardPlatingCost").alias("ClosingStandardPlatingCost"), col("in0.OpeningStandardUnitCost").alias("OpeningStandardUnitCost"), col("in1.ClosingPlannerCode").alias("ClosingPlannerCode"), col("in0.OpeningBuyer").alias("OpeningBuyer"), col("in0.OpeningPlatingCost").alias("OpeningPlatingCost"), col("in1.ClosingLaborCost").alias("ClosingLaborCost"), col("in0.OpeningOrderPolicy").alias("OpeningOrderPolicy"), col("in0.OpeningUnitCost").alias("OpeningUnitCost"), col("in1.ClosingBuyer").alias("ClosingBuyer"), col("in0.PartNumber").alias("PartNumber"), col("in0.OpeningPlannerCode").alias("OpeningPlannerCode"), col("in1.ClosingUnitCost").alias("ClosingUnitCost"), col("in1.ClosingOrderPolicy").alias("ClosingOrderPolicy"), col("in0.OpeningStandardLaborCost").alias("OpeningStandardLaborCost"), col("in0.OpeningLaborCost").alias("OpeningLaborCost"), col("in1.ClosingCustomerSupplied").alias("ClosingCustomerSupplied"), col("in1.ClostingPlatingCost").alias("ClostingPlatingCost"), col("in0.OpeningCustomerSupplied").alias("OpeningCustomerSupplied"), col("in0.OpeningStandardPlatingCost").alias("OpeningStandardPlatingCost"))
