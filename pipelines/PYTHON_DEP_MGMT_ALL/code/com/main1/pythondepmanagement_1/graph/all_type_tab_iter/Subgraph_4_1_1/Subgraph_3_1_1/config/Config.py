from com.main1.pythondepmanagement_1.graph.all_type_tab_iter.Subgraph_4_1_1.Subgraph_3_1_1.Subgraph_7_1_1\
    .config\
    .Config import SubgraphConfig as Subgraph_7_1_1_Config
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            Subgraph_7_1_1: dict={},
            c_sg3_1_1_c_long: int=22,
            c_string: str="testanother1",
            **kwargs
    ):
        self.Subgraph_7_1_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_7_1_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_7_1_1, 
            Subgraph_7_1_1_Config
        )
        self.c_sg3_1_1_c_long = c_sg3_1_1_c_long
        self.c_string = c_string
        pass

    def update(self, updated_config):
        self.Subgraph_7_1_1 = updated_config.Subgraph_7_1_1
        self.c_sg3_1_1_c_long = updated_config.c_sg3_1_1_c_long
        self.c_string = updated_config.c_string
        pass

Config = SubgraphConfig()
