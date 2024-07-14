from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AccountingTransactio_70(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("IsBOM", BooleanType(), True), StructField("PlannerCode", StringType(), True), StructField("AccountingPlannerCode", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Map.xlsx|||`Planners$`")
