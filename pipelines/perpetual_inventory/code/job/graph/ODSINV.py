from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ODSINV(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def read_file(file_name):
        return spark.read.format("csv").option("header", "true").load(file_name)

    data_frames = [read_file(row['fileName']) for row in df.select('fileName').collect()]
    out0 = reduce(DataFrame.union, data_frames)

    return out0
