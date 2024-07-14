from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_TextInput_1 = TextInput_1(spark)
    df_AdvanceEndDatetoNextDate = AdvanceEndDatetoNextDate(spark, df_TextInput_1)
    df_PreviousQuarterClosingDate = PreviousQuarterClosingDate(spark, df_AdvanceEndDatetoNextDate)
    df_Dates = Dates(spark, df_PreviousQuarterClosingDate)
    df_ODSINV_fileName = ODSINV_fileName(spark, df_Dates)
    df_ODSINV = ODSINV(spark, df_ODSINV_fileName)
    df_yxmc_108 = yxmc_108(spark, df_ODSINV)
    df_AlteryxSelect_109 = AlteryxSelect_109(spark, df_yxmc_108)
    df_PreviousQuarterClosingINV = PreviousQuarterClosingINV(spark, df_AlteryxSelect_109)
    df_AlteryxSelect_20 = AlteryxSelect_20(spark, df_AdvanceEndDatetoNextDate)
    df_Dates = Dates(spark, df_AlteryxSelect_20)
    df_DynamicInput_33_fileName = DynamicInput_33_fileName(spark, df_Dates)
    df_DynamicInput_33 = DynamicInput_33(spark, df_DynamicInput_33_fileName)
    df_SamtecFacilitieswith = SamtecFacilitieswith(spark)
    df_AlteryxSelect_173 = AlteryxSelect_173(spark, df_SamtecFacilitieswith)
    df_Filter_12 = Filter_12(spark, df_AlteryxSelect_173)
    df_AlteryxSelect_23 = AlteryxSelect_23(spark, df_Filter_12)
    df_Facilities = Facilities(spark, df_AlteryxSelect_23)
    df_Join_36_inner = Join_36_inner(spark, df_DynamicInput_33, df_Facilities)
    df_ClosingBalance = ClosingBalance(spark, df_Join_36_inner)
    df_AlteryxSelect_34 = AlteryxSelect_34(spark, df_ClosingBalance)
    df_Facilities = Facilities(spark, df_AlteryxSelect_23)
    df_Transactions_createData = Transactions_createData(spark)
    df_Transactions = Transactions(spark, df_Transactions_createData)
    df_AlteryxSelect_59 = AlteryxSelect_59(spark, df_Transactions)
    df_DatefromFileName = DatefromFileName(spark, df_AlteryxSelect_59)
    df_Dates = Dates(spark, df_AlteryxSelect_20)
    df_AppendFields_61 = AppendFields_61(spark, df_DatefromFileName, df_Dates)
    df_DateFilter = DateFilter(spark, df_AppendFields_61)
    df_AlteryxSelect_63 = AlteryxSelect_63(spark, df_DateFilter)
    df_Filter_183 = Filter_183(spark, df_AlteryxSelect_63)
    df_DynamicInput_64_fileName = DynamicInput_64_fileName(spark, df_Filter_183)
    df_DynamicInput_64 = DynamicInput_64(spark, df_DynamicInput_64_fileName)
    df_Filter_183_reject = Filter_183_reject(spark, df_AlteryxSelect_63)
    df_DynamicInput_184_fileName = DynamicInput_184_fileName(spark, df_Filter_183_reject)
    df_DynamicInput_184 = DynamicInput_184(spark, df_DynamicInput_184_fileName)
    df_Union_185 = Union_185(spark, df_DynamicInput_64, df_DynamicInput_184)
    df_Formula_178 = Formula_178(spark, df_Union_185)
    df_Join_65_inner = Join_65_inner(spark, df_Formula_178, df_Facilities)
    df_AlteryxSelect_67 = AlteryxSelect_67(spark, df_Join_65_inner)
    df_Dates = Dates(spark, df_PreviousQuarterClosingDate)
    df_ODSINV_fileName = ODSINV_fileName(spark, df_Dates)
    df_ClosingBalance = ClosingBalance(spark, df_AlteryxSelect_34)
    df_Summarize_158 = Summarize_158(spark, df_ClosingBalance)
    df_DSN_r2s_prod_edw_alt = DSN_r2s_prod_edw_alt(spark)
    df_AlteryxSelect_182 = AlteryxSelect_182(spark, df_DSN_r2s_prod_edw_alt)
    df_AccountingTransactio = AccountingTransactio(spark)
    df_Union_83_reformat_0 = Union_83_reformat_0(spark, df_AccountingTransactio)
    df_Transactions = Transactions(spark, df_AlteryxSelect_67)
    df_AccountingTransactio_71 = AccountingTransactio_71(spark)
    df_AlteryxSelect_76 = AlteryxSelect_76(spark, df_AccountingTransactio_71)
    df_AccountingTransactionTypes = AccountingTransactionTypes(spark, df_AlteryxSelect_76)
    df_Join_80_inner = Join_80_inner(spark, df_Transactions, df_AccountingTransactionTypes)
    df_CrossTab_81 = CrossTab_81(spark, df_Join_80_inner)
    df_AlteryxSelect_85 = AlteryxSelect_85(spark, df_CrossTab_81)
    df_Union_83_reformat_1 = Union_83_reformat_1(spark, df_AlteryxSelect_85)
    df_Union_83 = Union_83(spark, df_Union_83_reformat_0, df_Union_83_reformat_1)
    df_AlteryxSelect_86 = AlteryxSelect_86(spark, df_Union_83)
    df_yxmc_84 = yxmc_84(spark, df_AlteryxSelect_86)
    df_SummarizedTransactions = SummarizedTransactions(spark, df_yxmc_84)
    df_Balances_createData = Balances_createData(spark)
    df_Balances = Balances(spark, df_Balances_createData)
    df_Filter_51 = Filter_51(spark, df_Balances)
    df_AccountingTransactio_70 = AccountingTransactio_70(spark)
    df_AlteryxSelect_72 = AlteryxSelect_72(spark, df_AccountingTransactio_70)
    df_AccountingPlannerCodes = AccountingPlannerCodes(spark, df_AlteryxSelect_72)
    df_Dates = Dates(spark, df_PreviousQuarterClosingDate)
    df_ODSINV_fileName = ODSINV_fileName(spark, df_Dates)
    df_ODSINV = ODSINV(spark, df_ODSINV_fileName)
    df_yxmc_100 = yxmc_100(spark, df_ODSINV)
    df_AlteryxSelect_74 = AlteryxSelect_74(spark, df_AlteryxSelect_182)
    df_Part = Part(spark, df_AlteryxSelect_74)
    df_SummarizedTransactions = SummarizedTransactions(spark, df_SummarizedTransactions)
    df_Dates = Dates(spark, df_AlteryxSelect_20)
    df_DynamicInput_28_fileName = DynamicInput_28_fileName(spark, df_Dates)
    df_DynamicInput_28 = DynamicInput_28(spark, df_DynamicInput_28_fileName)
    df_Facilities = Facilities(spark, df_AlteryxSelect_23)
    df_Join_31_inner = Join_31_inner(spark, df_DynamicInput_28, df_Facilities)
    df_OpeningBalance = OpeningBalance(spark, df_Join_31_inner)
    df_AlteryxSelect_29 = AlteryxSelect_29(spark, df_OpeningBalance)
    df_OpeningBalance = OpeningBalance(spark, df_AlteryxSelect_29)
    df_Summarize_157 = Summarize_157(spark, df_OpeningBalance)
    df_Join_43_left_UnionFullOuter = Join_43_left_UnionFullOuter(spark, df_Summarize_157, df_Summarize_158)
    df_Join_87_left_UnionFullOuter = Join_87_left_UnionFullOuter(
        spark, 
        df_Join_43_left_UnionFullOuter, 
        df_SummarizedTransactions
    )
    df_AlteryxSelect_90 = AlteryxSelect_90(spark, df_Join_87_left_UnionFullOuter)
    df_yxmc_94 = yxmc_94(spark, df_AlteryxSelect_90)
    df_CalculatedClosingBalance = CalculatedClosingBalance(spark, df_yxmc_94)
    df_AlteryxSelect_101 = AlteryxSelect_101(spark, df_yxmc_100)
    df_OpeningINV = OpeningINV(spark, df_AlteryxSelect_101)
    df_ODSINV = ODSINV(spark, df_ODSINV_fileName)
    df_yxmc_104 = yxmc_104(spark, df_ODSINV)
    df_AlteryxSelect_105 = AlteryxSelect_105(spark, df_yxmc_104)
    df_ClosingINV = ClosingINV(spark, df_AlteryxSelect_105)
    df_Join_114_left_UnionFullOuter = Join_114_left_UnionFullOuter(spark, df_OpeningINV, df_ClosingINV)
    df_Join_115_left_UnionFullOuter = Join_115_left_UnionFullOuter(
        spark, 
        df_Join_114_left_UnionFullOuter, 
        df_PreviousQuarterClosingINV
    )
    df_yxmc_123 = yxmc_123(spark, df_Join_115_left_UnionFullOuter)
    df_Join_127_left_UnionLeftOuter = Join_127_left_UnionLeftOuter(spark, df_yxmc_123, df_AccountingPlannerCodes)
    df_Join_129_left_UnionLeftOuter = Join_129_left_UnionLeftOuter(
        spark, 
        df_Join_127_left_UnionLeftOuter, 
        df_AccountingPlannerCodes
    )
    df_Join_130_left_UnionLeftOuter = Join_130_left_UnionLeftOuter(
        spark, 
        df_Join_129_left_UnionLeftOuter, 
        df_AccountingPlannerCodes
    )
    df_AlteryxSelect_118 = AlteryxSelect_118(spark, df_Join_130_left_UnionLeftOuter)
    df_CostsCodes = CostsCodes(spark, df_AlteryxSelect_118)
    df_Join_122_left_UnionLeftOuter = Join_122_left_UnionLeftOuter(spark, df_CalculatedClosingBalance, df_CostsCodes)
    df_Join_138_inner = Join_138_inner(spark, df_Join_122_left_UnionLeftOuter, df_Part)
    df_AlteryxSelect_133 = AlteryxSelect_133(spark, df_Join_138_inner)
    df_OpeningClosingValues = OpeningClosingValues(spark, df_AlteryxSelect_133)
    df_AlteryxSelect_135 = AlteryxSelect_135(spark, df_OpeningClosingValues)
    df_AlteryxSelect_48 = AlteryxSelect_48(spark, df_Filter_51)
    df_DatefromFileName = DatefromFileName(spark, df_AlteryxSelect_48)
    df_Summarize_50 = Summarize_50(spark, df_DatefromFileName)
    df_AlteryxSelect_140 = AlteryxSelect_140(spark, df_AlteryxSelect_135)
    PerpetualInventory_y(spark, df_AlteryxSelect_140)
    df_DatesfromUserInput = DatesfromUserInput(spark, df_AlteryxSelect_20)
    df_AppendFields_52 = AppendFields_52(spark, df_Summarize_50, df_DatesfromUserInput)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/perpetual_inventory")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/perpetual_inventory", config = Config)(pipeline)

if __name__ == "__main__":
    main()
