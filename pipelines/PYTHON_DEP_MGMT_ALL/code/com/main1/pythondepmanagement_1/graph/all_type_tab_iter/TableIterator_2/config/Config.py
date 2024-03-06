from com.main1.pythondepmanagement_1.graph.all_type_tab_iter.TableIterator_2.TableIterator_3.config.Config import (
    SubgraphConfig as TableIterator_3_Config
)
from prophecy.config import ConfigBase


class C_struct(ConfigBase):
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
            c_date: str="",
            c_timestamp: str="",
            c_array_int: list=[],
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
        self.c_date = c_date
        self.c_timestamp = c_timestamp
        self.c_array_int = c_array_int
        pass


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
            c_date: str="",
            c_timestamp: str="",
            c_array_int: list=[],
            c_array_string: list=[],
            c_array_long: list=[],
            c_array_boolean: list=[],
            c_array_date: list=[],
            c_array_timestamp: list=[],
            c_array_float: list=[],
            c_array_decimal: list=[],
            c_struct: dict={},
            TableIterator_3: dict={},
            c_test_config: str="concat(a, c_int)",
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
        self.c_date = c_date
        self.c_timestamp = c_timestamp
        self.c_array_int = c_array_int
        self.c_array_string = c_array_string
        self.c_array_long = c_array_long
        self.c_array_boolean = c_array_boolean
        self.c_array_date = c_array_date
        self.c_array_timestamp = c_array_timestamp
        self.c_array_float = c_array_float
        self.c_array_decimal = c_array_decimal
        self.c_struct = self.get_config_object(
            prophecy_spark, 
            C_struct(prophecy_spark = prophecy_spark), 
            c_struct, 
            C_struct
        )
        self.TableIterator_3 = self.get_config_object(
            prophecy_spark, 
            TableIterator_3_Config(prophecy_spark = prophecy_spark), 
            TableIterator_3, 
            TableIterator_3_Config
        )
        self.c_test_config = c_test_config
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
        self.c_date = updated_config.c_date
        self.c_timestamp = updated_config.c_timestamp
        self.c_array_int = updated_config.c_array_int
        self.c_array_string = updated_config.c_array_string
        self.c_array_long = updated_config.c_array_long
        self.c_array_boolean = updated_config.c_array_boolean
        self.c_array_date = updated_config.c_array_date
        self.c_array_timestamp = updated_config.c_array_timestamp
        self.c_array_float = updated_config.c_array_float
        self.c_array_decimal = updated_config.c_array_decimal
        self.c_struct = updated_config.c_struct
        self.TableIterator_3 = updated_config.TableIterator_3
        self.c_test_config = updated_config.c_test_config
        pass

Config = SubgraphConfig()
