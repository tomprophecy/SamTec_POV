from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            jdbcUrl_SamtecFacilitieswith: str=None,
            username_SamtecFacilitieswith: str=None,
            password_SamtecFacilitieswith: str=None,
            jdbcUrl_PerpetualInventoryDe: str=None,
            username_PerpetualInventoryDe: str=None,
            password_PerpetualInventoryDe: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            jdbcUrl_SamtecFacilitieswith, 
            username_SamtecFacilitieswith, 
            password_SamtecFacilitieswith, 
            jdbcUrl_PerpetualInventoryDe, 
            username_PerpetualInventoryDe, 
            password_PerpetualInventoryDe
        )

    def update(
            self,
            jdbcUrl_SamtecFacilitieswith: str="X:\\adw\\operations\\material management\\material reconciliation\\Samtec Facilities with Balances.yxdb",
            username_SamtecFacilitieswith: str="",
            password_SamtecFacilitieswith: str="",
            jdbcUrl_PerpetualInventoryDe: str="Perpetual Inventory Details.yxdb",
            username_PerpetualInventoryDe: str="",
            password_PerpetualInventoryDe: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.jdbcUrl_SamtecFacilitieswith = jdbcUrl_SamtecFacilitieswith
        self.username_SamtecFacilitieswith = username_SamtecFacilitieswith
        self.password_SamtecFacilitieswith = password_SamtecFacilitieswith
        self.jdbcUrl_PerpetualInventoryDe = jdbcUrl_PerpetualInventoryDe
        self.username_PerpetualInventoryDe = username_PerpetualInventoryDe
        self.password_PerpetualInventoryDe = password_PerpetualInventoryDe
        pass
