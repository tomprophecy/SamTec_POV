from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SamtecFacilitieswith(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_SamtecFacilitieswith}")\
        .option("user", f"{Config.username_SamtecFacilitieswith}")\
        .option("password", f"{Config.password_SamtecFacilitieswith}")\
        .option(
          "query",
          "X:\\adw\\operations\\material management\\material reconciliation\\Samtec Facilities with Balances.yxdb"
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
