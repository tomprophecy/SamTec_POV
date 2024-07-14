from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_81_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.PartNumber") == col("in1.PartNumber")), "inner")\
        .select(col("in0.Plant").alias("Plant"), col("in0.Quantity").alias("Quantity"), col("in0.TransactionId").alias("TransactionId"), col("in0.TransactionType").alias("TransactionType"), col("in0.PartNumber").alias("PartNumber"), col("in0.Note").alias("Note"), col("in0.Facility").alias("Facility"), col("in0.DateTransacted").alias("DateTransacted"), col("in0.TransactedBy").alias("TransactedBy"))
