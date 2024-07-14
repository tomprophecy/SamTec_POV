from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            jdbcUrl_SamtecFacilitieswith: str=None,
            username_SamtecFacilitieswith: str=None,
            password_SamtecFacilitieswith: str=None,
            jdbcUrl_DSN_r2s_prod_edw_alt: str=None,
            username_DSN_r2s_prod_edw_alt: str=None,
            password_DSN_r2s_prod_edw_alt: str=None,
            jdbcUrl_AccountingTransactio: str=None,
            username_AccountingTransactio: str=None,
            password_AccountingTransactio: str=None,
            jdbcUrl_PerpetualInventory_y: str=None,
            username_PerpetualInventory_y: str=None,
            password_PerpetualInventory_y: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            jdbcUrl_SamtecFacilitieswith, 
            username_SamtecFacilitieswith, 
            password_SamtecFacilitieswith, 
            jdbcUrl_DSN_r2s_prod_edw_alt, 
            username_DSN_r2s_prod_edw_alt, 
            password_DSN_r2s_prod_edw_alt, 
            jdbcUrl_AccountingTransactio, 
            username_AccountingTransactio, 
            password_AccountingTransactio, 
            jdbcUrl_PerpetualInventory_y, 
            username_PerpetualInventory_y, 
            password_PerpetualInventory_y
        )

    def update(
            self,
            jdbcUrl_SamtecFacilitieswith: str="X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Samtec Facilities with Balances.yxdb",
            username_SamtecFacilitieswith: str="",
            password_SamtecFacilitieswith: str="",
            jdbcUrl_DSN_r2s_prod_edw_alt: str="odbc:DSN=r2s-prod-edw-alteryx",
            username_DSN_r2s_prod_edw_alt: str="",
            password_DSN_r2s_prod_edw_alt: str="",
            jdbcUrl_AccountingTransactio: str="X:\\ADW\\Operations\\Material Management\\Material Reconciliation\\Accounting Transaction Type Schema.yxdb",
            username_AccountingTransactio: str="",
            password_AccountingTransactio: str="",
            jdbcUrl_PerpetualInventory_y: str="Perpetual Inventory.yxdb",
            username_PerpetualInventory_y: str="",
            password_PerpetualInventory_y: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.jdbcUrl_SamtecFacilitieswith = jdbcUrl_SamtecFacilitieswith
        self.username_SamtecFacilitieswith = username_SamtecFacilitieswith
        self.password_SamtecFacilitieswith = password_SamtecFacilitieswith
        self.jdbcUrl_DSN_r2s_prod_edw_alt = jdbcUrl_DSN_r2s_prod_edw_alt
        self.username_DSN_r2s_prod_edw_alt = username_DSN_r2s_prod_edw_alt
        self.password_DSN_r2s_prod_edw_alt = password_DSN_r2s_prod_edw_alt
        self.jdbcUrl_AccountingTransactio = jdbcUrl_AccountingTransactio
        self.username_AccountingTransactio = username_AccountingTransactio
        self.password_AccountingTransactio = password_AccountingTransactio
        self.jdbcUrl_PerpetualInventory_y = jdbcUrl_PerpetualInventory_y
        self.username_PerpetualInventory_y = username_PerpetualInventory_y
        self.password_PerpetualInventory_y = password_PerpetualInventory_y
        pass
