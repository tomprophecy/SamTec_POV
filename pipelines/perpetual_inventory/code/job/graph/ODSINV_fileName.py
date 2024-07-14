from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def ODSINV_fileName(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "fileName",
        lit(
          "concat('aka:64a2f038381c38b000cdece3|||select ITEMDESC as PartNumber,\n\tFRZCOST as UnitCost,\n\tFRZ5 + PREVFRZ5 as LaborCost,\n\tFRZ3 + PREVFRZ3 as PlatingCost,\n\tPLANNER as PlannerCode,\n\tBUYER as Buyer,\n\tORDPOL as OrderPolicy,\n\tMISCSTAT3 as CustomerSupplied,\n\tSTDCOST as StandardCost,\n\tCOST5 + PREVSTD5 as StandardLaborCost,\n\tCOST3 + PREVSTD3 as StandardPlatingCost \nfrom ODS.Analysis.tvf_bridging__dbo__PCMFG_INV('1975-12-30')', [])"
        )
    )
