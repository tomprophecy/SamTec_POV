from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_33(spark: SparkSession) -> DataFrame:
    
    colNames = ["PartNumber"]
    data = [["BW-44WB7-SPC"]]
    rows = [Row(*row) for row in data]
    schema = StructType([
StructField("PartNumber", StringType(), nullable = True)])
    out0 = spark.createDataFrame(rows, schema)

    return out0
