from prophecy.config import ConfigBase


class Arr1(ConfigBase):
    def __init__(self, prophecy_spark=None, test123: str=None, **kwargs):
        self.test123 = test123
        pass


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, arr1: list=None, strt: str="test123", **kwargs):
        self.arr1 = self.get_config_object(
            prophecy_spark, 
            [Arr1(prophecy_spark = prophecy_spark, test123 = "check")], 
            arr1, 
            Arr1
        )
        self.strt = strt
        pass

    def update(self, updated_config):
        self.arr1 = updated_config.arr1
        self.strt = updated_config.strt
        pass

Config = SubgraphConfig()
