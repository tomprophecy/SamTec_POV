from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AccountingTransactio(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_AccountingTransactio}")\
        .option("user", f"{Config.username_AccountingTransactio}")\
        .option("password", f"{Config.password_AccountingTransactio}")\
        .option(
          "query",
          "X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Type Schema.yxdb"
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
