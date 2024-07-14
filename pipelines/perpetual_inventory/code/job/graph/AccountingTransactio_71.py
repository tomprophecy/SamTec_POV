from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AccountingTransactio_71(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Base Transaction Type", StringType(), True), StructField("Accounting Transaction Type", StringType(), True)
        ])
        )\
        .option("header", False)\
        .option("quote", "\"")\
        .option("sep", "")\
        .csv("X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Map.xlsx|||`Transaction Types$`")
