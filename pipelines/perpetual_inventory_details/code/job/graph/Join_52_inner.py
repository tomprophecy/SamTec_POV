from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_52_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.Facility") == col("in1.Facility")), "inner")\
        .select(col("in0.Plant").alias("Plant"), col("in0.Timestamp").alias("Timestamp"), col("in0.WIPOnHandBalance").alias("WIPOnHandBalance"), col("in0.BentecOnHandBalance").alias("BentecOnHandBalance"), col("in0.PartNumber").alias("PartNumber"), col("in0.WarehouseOnHandBalance").alias("WarehouseOnHandBalance"), col("in0.ConsignedOnHandBalance").alias("ConsignedOnHandBalance"), col("in0.Facility").alias("Facility"))
