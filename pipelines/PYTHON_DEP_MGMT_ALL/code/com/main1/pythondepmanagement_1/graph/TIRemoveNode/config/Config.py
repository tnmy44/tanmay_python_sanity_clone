from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            c_short: int=0,
            c_int: int=0,
            c_long: int=0,
            c_decimal: float=0.0,
            c_float: float=0.0,
            c_boolean: bool=True,
            c_double: float=0.0,
            c_string: str="",
            CONFIG_INT: int=22,
            **kwargs
    ):
        self.c_short = c_short
        self.c_int = c_int
        self.c_long = c_long
        self.c_decimal = c_decimal
        self.c_float = c_float
        self.c_boolean = c_boolean
        self.c_double = c_double
        self.c_string = c_string
        self.CONFIG_INT = CONFIG_INT
        pass

    def update(self, updated_config):
        self.c_short = updated_config.c_short
        self.c_int = updated_config.c_int
        self.c_long = updated_config.c_long
        self.c_decimal = updated_config.c_decimal
        self.c_float = updated_config.c_float
        self.c_boolean = updated_config.c_boolean
        self.c_double = updated_config.c_double
        self.c_string = updated_config.c_string
        self.CONFIG_INT = updated_config.CONFIG_INT
        pass

Config = SubgraphConfig()
