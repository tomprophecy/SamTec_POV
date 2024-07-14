from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_AccountingTransactio = AccountingTransactio(spark)
    df_Transactions_createData = Transactions_createData(spark)
    df_Balances_createData = Balances_createData(spark)
    df_Balances = Balances(spark, df_Balances_createData)
    df_Filter_15 = Filter_15(spark, df_Balances)
    df_AlteryxSelect_12 = AlteryxSelect_12(spark, df_Filter_15)
    df_TextInput_33 = TextInput_33(spark)
    df_AlteryxSelect_37 = AlteryxSelect_37(spark, df_TextInput_33)
    df_PartNumber = PartNumber(spark, df_AlteryxSelect_37)
    df_Transactions = Transactions(spark, df_Transactions_createData)
    df_AlteryxSelect_59 = AlteryxSelect_59(spark, df_Transactions)
    df_DatefromFileName = DatefromFileName(spark, df_AlteryxSelect_59)
    df_TextInput_26 = TextInput_26(spark)
    df_AdvanceEndDatetoNextDate = AdvanceEndDatetoNextDate(spark, df_TextInput_26)
    df_AlteryxSelect_28 = AlteryxSelect_28(spark, df_AdvanceEndDatetoNextDate)
    df_Dates = Dates(spark, df_AlteryxSelect_28)
    df_AppendFields_60 = AppendFields_60(spark, df_DatefromFileName, df_Dates)
    df_DateFilter = DateFilter(spark, df_AppendFields_60)
    df_AlteryxSelect_62 = AlteryxSelect_62(spark, df_DateFilter)
    df_DynamicInput_63_fileName = DynamicInput_63_fileName(spark, df_AlteryxSelect_62)
    df_DynamicInput_63 = DynamicInput_63(spark, df_DynamicInput_63_fileName)
    df_SamtecFacilitieswith = SamtecFacilitieswith(spark)
    df_AlteryxSelect_107 = AlteryxSelect_107(spark, df_SamtecFacilitieswith)
    df_Filter_23 = Filter_23(spark, df_AlteryxSelect_107)
    df_AlteryxSelect_24 = AlteryxSelect_24(spark, df_Filter_23)
    df_Facilities = Facilities(spark, df_AlteryxSelect_24)
    df_Join_64_inner = Join_64_inner(spark, df_DynamicInput_63, df_Facilities)
    df_Join_81_inner = Join_81_inner(spark, df_Join_64_inner, df_PartNumber)
    df_AlteryxSelect_66 = AlteryxSelect_66(spark, df_Join_81_inner)
    df_Facilities = Facilities(spark, df_AlteryxSelect_24)
    df_PartNumber = PartNumber(spark, df_AlteryxSelect_37)
    df_Dates = Dates(spark, df_AlteryxSelect_28)
    df_DynamicInput_40_fileName = DynamicInput_40_fileName(spark, df_Dates)
    df_DynamicInput_40 = DynamicInput_40(spark, df_DynamicInput_40_fileName)
    df_Facilities = Facilities(spark, df_AlteryxSelect_24)
    df_Join_43_inner = Join_43_inner(spark, df_DynamicInput_40, df_Facilities)
    df_Join_75_inner = Join_75_inner(spark, df_Join_43_inner, df_PartNumber)
    df_OpeningBalance = OpeningBalance(spark, df_Join_75_inner)
    df_AlteryxSelect_41 = AlteryxSelect_41(spark, df_OpeningBalance)
    df_OpeningBalance = OpeningBalance(spark, df_AlteryxSelect_41)
    df_AlteryxSelect_100 = AlteryxSelect_100(spark, df_AccountingTransactio)
    df_PartNumber = PartNumber(spark, df_AlteryxSelect_37)
    df_Transactions = Transactions(spark, df_AlteryxSelect_66)
    df_Summarize_108 = Summarize_108(spark, df_OpeningBalance)
    df_Dates = Dates(spark, df_AlteryxSelect_28)
    df_DynamicInput_49_fileName = DynamicInput_49_fileName(spark, df_Dates)
    df_DynamicInput_49 = DynamicInput_49(spark, df_DynamicInput_49_fileName)
    df_Join_52_inner = Join_52_inner(spark, df_DynamicInput_49, df_Facilities)
    df_Join_76_inner = Join_76_inner(spark, df_Join_52_inner, df_PartNumber)
    df_ClosingBalance = ClosingBalance(spark, df_Join_76_inner)
    df_AlteryxSelect_50 = AlteryxSelect_50(spark, df_ClosingBalance)
    df_ClosingBalance = ClosingBalance(spark, df_AlteryxSelect_50)
    df_Summarize_109 = Summarize_109(spark, df_ClosingBalance)
    df_Join_88_left_UnionFullOuter = Join_88_left_UnionFullOuter(spark, df_Summarize_108, df_Summarize_109)
    df_Join_91_left_UnionFullOuter = Join_91_left_UnionFullOuter(spark, df_Join_88_left_UnionFullOuter, df_Transactions)
    df_yxmc_94 = yxmc_94(spark, df_Join_91_left_UnionFullOuter)
    df_Formula_92 = Formula_92(spark, df_yxmc_94)
    df_Sort_95 = Sort_95(spark, df_Formula_92)
    df_MultiRowFormula_96 = MultiRowFormula_96(spark, df_Sort_95)
    df_AlteryxSelect_97 = AlteryxSelect_97(spark, df_MultiRowFormula_96)
    df_AccountingTransactionTypes = AccountingTransactionTypes(spark, df_AlteryxSelect_100)
    df_Join_99_inner = Join_99_inner(spark, df_AlteryxSelect_97, df_AccountingTransactionTypes)
    df_AlteryxSelect_103 = AlteryxSelect_103(spark, df_Join_99_inner)
    df_Sort_104 = Sort_104(spark, df_AlteryxSelect_103)
    df_DatefromFileName = DatefromFileName(spark, df_AlteryxSelect_12)
    df_Summarize_14 = Summarize_14(spark, df_DatefromFileName)
    df_DatesfromUserInput = DatesfromUserInput(spark, df_AlteryxSelect_28)
    df_AppendFields_16 = AppendFields_16(spark, df_Summarize_14, df_DatesfromUserInput)
    df_PreviousQuarterClosingDate = PreviousQuarterClosingDate(spark, df_AdvanceEndDatetoNextDate)
    PerpetualInventoryDe(spark, df_Sort_104)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/perpetual_inventory_details")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/perpetual_inventory_details", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
