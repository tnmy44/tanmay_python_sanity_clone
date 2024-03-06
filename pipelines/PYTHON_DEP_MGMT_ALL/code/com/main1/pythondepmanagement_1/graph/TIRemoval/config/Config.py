from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            a: int=0,
            C_NUM: float=0.0,
            C_NUM10: float=0.0,
            C_DOUBLE: float=0.0,
            C_STRING: str="",
            C_TIMESTAMP: str="",
            C_DATE: str="",
            C_BOOL: bool=True,
            C_OBJECT: str="",
            CONFIG_INT: int=22,
            **kwargs
    ):
        self.a = a
        self.C_NUM = C_NUM
        self.C_NUM10 = C_NUM10
        self.C_DOUBLE = C_DOUBLE
        self.C_STRING = C_STRING
        self.C_TIMESTAMP = C_TIMESTAMP
        self.C_DATE = C_DATE
        self.C_BOOL = C_BOOL
        self.C_OBJECT = C_OBJECT
        self.CONFIG_INT = CONFIG_INT
        pass

    def update(self, updated_config):
        self.a = updated_config.a
        self.C_NUM = updated_config.C_NUM
        self.C_NUM10 = updated_config.C_NUM10
        self.C_DOUBLE = updated_config.C_DOUBLE
        self.C_STRING = updated_config.C_STRING
        self.C_TIMESTAMP = updated_config.C_TIMESTAMP
        self.C_DATE = updated_config.C_DATE
        self.C_BOOL = updated_config.C_BOOL
        self.C_OBJECT = updated_config.C_OBJECT
        self.CONFIG_INT = updated_config.CONFIG_INT
        pass

Config = SubgraphConfig()
