from prophecy.config import ConfigBase


class C4(ConfigBase):
    def __init__(self, prophecy_spark=None, stry1: str=None, **kwargs):
        self.stry1 = stry1
        pass


class C3_arr(ConfigBase):
    def __init__(self, prophecy_spark=None, c4: list=None, **kwargs):
        self.c4 = self.get_config_object(prophecy_spark, [], c4, C4)
        pass


class C2(ConfigBase):
    def __init__(self, prophecy_spark=None, c3_arr: list=None, **kwargs):
        self.c3_arr = self.get_config_object(
            prophecy_spark, 
            [C3_arr(prophecy_spark = prophecy_spark, c4 = [C4(prophecy_spark = prophecy_spark, stry1 = "def value")])], 
            c3_arr, 
            C3_arr
        )
        pass


class C1(ConfigBase):
    def __init__(self, prophecy_spark=None, c2: dict={}, **kwargs):
        self.c2 = self.get_config_object(prophecy_spark, C2(prophecy_spark = prophecy_spark), c2, C2)
        pass


class Config(ConfigBase):

    def __init__(self, c1: dict=None, **kwargs):
        self.spark = None
        self.update(c1)

    def update(self, c1: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.c1 = self.get_config_object(prophecy_spark, C1(prophecy_spark = prophecy_spark), c1, C1)
        pass
