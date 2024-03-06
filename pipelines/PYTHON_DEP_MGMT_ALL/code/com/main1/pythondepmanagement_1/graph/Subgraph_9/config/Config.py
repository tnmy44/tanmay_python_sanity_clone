from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            path_var: str="dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv",
            **kwargs
    ):
        self.path_var = path_var
        pass

    def update(self, updated_config):
        self.path_var = updated_config.path_var
        pass

Config = SubgraphConfig()
