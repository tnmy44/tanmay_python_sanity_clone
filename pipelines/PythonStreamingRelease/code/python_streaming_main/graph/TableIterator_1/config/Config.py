from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            customer_id: str="",
            first_name: str="",
            last_name: str="",
            phone: str="",
            email: str="",
            country_code: str="",
            account_open_date: str="",
            account_flags: str="",
            **kwargs
    ):
        self.customer_id = customer_id
        self.first_name = first_name
        self.last_name = last_name
        self.phone = phone
        self.email = email
        self.country_code = country_code
        self.account_open_date = account_open_date
        self.account_flags = account_flags
        pass

    def update(self, updated_config):
        self.customer_id = updated_config.customer_id
        self.first_name = updated_config.first_name
        self.last_name = updated_config.last_name
        self.phone = updated_config.phone
        self.email = updated_config.email
        self.country_code = updated_config.country_code
        self.account_open_date = updated_config.account_open_date
        self.account_flags = updated_config.account_flags
        pass

Config = SubgraphConfig()
