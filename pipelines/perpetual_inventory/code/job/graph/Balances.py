from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Balances(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "fileNameArray",
          call_spark_fcn(
            "directory_listing", 
            lit("X:ADWOperationsMaterial ManagementMaterial ReconciliationBalances"), 
            lit("")
          )
        )\
        .withColumn("fileName", explode(col("fileNameArray")))\
        .drop("fileNameArray")\
        .drop("seq")
