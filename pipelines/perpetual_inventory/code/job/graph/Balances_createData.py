from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Balances_createData(spark: SparkSession) -> DataFrame:
    from prophecy.utils.transpiler.abi_fcn_wrapper import call_spark_fcn

    return call_spark_fcn("generateDataFrameWithSequenceColumn", 1, 1, "seq", spark)
