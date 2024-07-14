from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextInput_1(spark: SparkSession) -> DataFrame:
    
    colNames = ["StartDate", "EndDate"]
    data = [["2023-01-01", "2023-01-31"]]
    rows = [Row(*row) for row in data]
    schema = StructType([
            StructField("StartDate", StringType(), nullable = True),
         StructField("EndDate", StringType(), nullable = True)

    ])
    out0 = spark.createDataFrame(rows, schema)

    return out0
