from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def PerpetualInventory_y(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_PerpetualInventory_y}")\
        .option("user", f"{Config.username_PerpetualInventory_y}")\
        .option("password", f"{Config.password_PerpetualInventory_y}")\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .mode("read")\
        .save()
