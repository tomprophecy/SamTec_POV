from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CalculatedClosingBalance(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "TransactedClosingBalance",
          (
            (
              (
                (
                  (
                    (
                      (
                        col("OpeningBalance")
                        + col("Purchased")
                      )
                      + col("Produced")
                    )
                    + col("Sold")
                  )
                  + col("Issued")
                )
                + col("Adjusted")
              )
              + col("TransferredIn")
            )
            + col("TransferredOut")
          )
        )\
        .withColumn("Reconciles", (abs((col("TransactedClosingBalance") - col("ClosingBalance"))).cast(IntegerType()) <= lit(1)))\
        .withColumn("Discrepancy", (col("TransactedClosingBalance") - col("ClosingBalance")))
