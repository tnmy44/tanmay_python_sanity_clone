from com.main1.pythondepmanagement_1.graph.Subgraph_1.Subgraph_4_1_1_1.config.Config import (
    SubgraphConfig as Subgraph_4_1_1_1_Config
)
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            Subgraph_4_1_1_1: dict={},
            config_int_here: int=20,
            JDBC_DATABASE: str="test_database",
            **kwargs
    ):
        self.Subgraph_4_1_1_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_4_1_1_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_4_1_1_1, 
            Subgraph_4_1_1_1_Config
        )
        self.config_int_here = config_int_here
        self.JDBC_DATABASE = JDBC_DATABASE
        pass

    def update(self, updated_config):
        self.Subgraph_4_1_1_1 = updated_config.Subgraph_4_1_1_1
        self.config_int_here = updated_config.config_int_here
        self.JDBC_DATABASE = updated_config.JDBC_DATABASE
        pass

Config = SubgraphConfig()
