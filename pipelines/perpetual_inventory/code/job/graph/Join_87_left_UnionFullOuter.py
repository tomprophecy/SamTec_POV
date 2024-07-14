from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_87_left_UnionFullOuter(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          (
            ((col("in0.Facility") == col("in1.Facility")) & (col("in0.Plant") == col("in1.Plant")))
            & (col("in0.PartNumber") == col("in1.PartNumber"))
          ),
          "fullouter"
        )\
        .select(col("in0.Plant").alias("Plant"), col("in0.ClosingBalance").alias("ClosingBalance"), col("in1.Produced").alias("Produced"), col("in1.Adjusted").alias("Adjusted"), col("in1.TransferredOut").alias("TransferredOut"), col("in1.Issued").alias("Issued"), col("in0.PartNumber").alias("PartNumber"), col("in1.TransferredIn").alias("TransferredIn"), col("in0.OpeningBalance").alias("OpeningBalance"), col("in1.Purchased").alias("Purchased"), col("in0.Facility").alias("Facility"), col("in1.Sold").alias("Sold"))
