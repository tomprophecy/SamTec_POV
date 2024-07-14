from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def DSN_r2s_prod_edw_alt(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", f"{Config.jdbcUrl_DSN_r2s_prod_edw_alt}")\
        .option("user", f"{Config.username_DSN_r2s_prod_edw_alt}")\
        .option("password", f"{Config.password_DSN_r2s_prod_edw_alt}")\
        .option(
          "query",
          """select common.part.part_number,
\tcommon.part.reporting_series,
\tcommon.part.reporting_series_era,
\tcommon.part.reporting_series_weave,
\tcommon.part.reporting_series_block,
\tcommon.part.reporting_series_segment,
\tcommon.part.dm_item_type,
\tcommon.part.pcmfg_item_type 
from common.part"""
        )\
        .option("pushDownPredicate", True)\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .load()
