from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, a: int=0, **kwargs):
        self.a = a
        pass

    def update(self, updated_config):
        self.a = updated_config.a
        pass

Config = SubgraphConfig()
