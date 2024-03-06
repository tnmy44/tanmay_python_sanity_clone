from pythonbasic.test.mainone.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class C_struct_another(ConfigBase):
    def __init__(self, prophecy_spark=None, csa_boolean: bool=True, **kwargs):
        self.csa_boolean = csa_boolean
        pass


class C_struct(ConfigBase):
    def __init__(
            self,
            prophecy_spark=None,
            c_array1: list=["1", "2"],
            c_struct_another: dict={},
            c_string: str="asd",
            **kwargs
    ):
        self.c_array1 = c_array1
        self.c_struct_another = self.get_config_object(
            prophecy_spark, 
            C_struct_another(prophecy_spark = prophecy_spark), 
            c_struct_another, 
            C_struct_another
        )
        self.c_string = c_string
        pass


class Config(ConfigBase):

    def __init__(
            self,
            c_string: str=None,
            Subgraph_1: dict=None,
            c_int: int=None,
            c_struct: dict=None,
            sfs: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(c_string, Subgraph_1, c_int, c_struct, sfs)

    def update(
            self,
            c_string: str="PYTHON_BASIC - DEFAULT",
            Subgraph_1: dict={},
            c_int: int=1,
            c_struct: dict={},
            sfs: str="  max-height: 50px;\nsdfsdf\nsdfsdffsdsdfds fds fsd fsd fsdfsdfsdfsdfdsfsdfsdfsdfsdfsdfdsfdsfdsfdsfdsfdsfdsfdsfsdfsdfsdfsdfsdfdsfdsfdsfdsfsdfdsfsdfsdfsdfdsfsdfsdfsdfsdfdsfsdfsdfsd\nfsdfsdfsd\nfsdfsdfsd  max-height: 50px;\nsdfsdf\nsdfsdffsd\nfsdfsdfsd\nfsdfsdfsd  max-height: 50px;\nsdfsdf\nsdfsdffsd\nfsdfsdfsd\nfsdfsdfsd\n\n\n",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.c_string = c_string
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.c_int = self.get_int_value(c_int)
        self.c_struct = self.get_config_object(
            prophecy_spark, 
            C_struct(prophecy_spark = prophecy_spark), 
            c_struct, 
            C_struct
        )
        self.sfs = sfs
        pass
