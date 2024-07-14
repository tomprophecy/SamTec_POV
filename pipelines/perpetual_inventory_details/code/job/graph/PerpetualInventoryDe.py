from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PerpetualInventoryDe(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_PerpetualInventoryDe}")\
        .option("user", f"{Config.username_PerpetualInventoryDe}")\
        .option("password", f"{Config.password_PerpetualInventoryDe}")\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .mode("read")\
        .save()
