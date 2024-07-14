from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PreviousQuarterClosingDate(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "PreviousQuarterClosingDate",
        when(
            array_contains(
              array(lit(2), lit(5), lit(8), lit(11)), 
              date_format(col("EndDate"), "MM").cast(IntegerType()).cast(DoubleType())
            ), 
            add_months(col("EndDate"), lit(- 1))
          )\
          .when(
            array_contains(
              array(lit(3), lit(6), lit(9), lit(12)), 
              date_format(col("EndDate"), "MM").cast(IntegerType()).cast(DoubleType())
            ), 
            add_months(col("EndDate"), lit(- 2))
          )\
          .otherwise(add_months(col("EndDate"), lit(- 3)))
    )
