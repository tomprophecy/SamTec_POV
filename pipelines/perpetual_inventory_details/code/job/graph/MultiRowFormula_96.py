from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_96(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        RollingBalance_lag1 = 0.0

        for row in iterator:
            OpeningBalance = row["OpeningBalance"]
            Quantity = row["Quantity"]
            RollingBalance = row["RollingBalance"]
            RollingBalance_new = (
                OpeningBalance
                + Quantity
            ) if ((not RollingBalance_lag1 == None) and isnull(RollingBalance_lag1)) else RollingBalance_lag1 + Quantity
            RollingBalance_lag1 = RollingBalance_new
            newRow = list(row)
            newRow[row.__fields__.index("RollingBalance")] = RollingBalance_new
            yield newRow

    resultRDD = in0\
        .repartition(col("Facility"), col("Plant"), col("PartNumber"))\
        .sortWithinPartitions(col("Facility"), col("Plant"), col("PartNumber"))\
        .rdd\
        .mapPartitions(
        calculate
    )
    out0 = spark.createDataFrame(resultRDD, in0.schema)

    return out0
