from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            c_sg1_c_string: str="dasdasdads",
            JDBC_DATABASE: str="test_database",
            **kwargs
    ):
        self.c_sg1_c_string = c_sg1_c_string
        self.JDBC_DATABASE = JDBC_DATABASE
        pass

    def update(self, updated_config):
        self.c_sg1_c_string = updated_config.c_sg1_c_string
        self.JDBC_DATABASE = updated_config.JDBC_DATABASE
        pass

Config = SubgraphConfig()
