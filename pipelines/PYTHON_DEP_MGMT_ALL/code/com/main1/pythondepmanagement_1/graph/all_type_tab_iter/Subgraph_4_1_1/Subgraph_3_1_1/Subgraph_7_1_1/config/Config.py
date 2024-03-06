from com.main1.pythondepmanagement_1.graph.all_type_tab_iter.Subgraph_4_1_1.Subgraph_3_1_1.Subgraph_7_1_1\
    .Subgraph_8_1_1\
    .config\
    .Config import SubgraphConfig as Subgraph_8_1_1_Config
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, Subgraph_8_1_1: dict={}, c_sg7_1_1_short: int=22, **kwargs):
        self.Subgraph_8_1_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_8_1_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_8_1_1, 
            Subgraph_8_1_1_Config
        )
        self.c_sg7_1_1_short = c_sg7_1_1_short
        pass

    def update(self, updated_config):
        self.Subgraph_8_1_1 = updated_config.Subgraph_8_1_1
        self.c_sg7_1_1_short = updated_config.c_sg7_1_1_short
        pass

Config = SubgraphConfig()
