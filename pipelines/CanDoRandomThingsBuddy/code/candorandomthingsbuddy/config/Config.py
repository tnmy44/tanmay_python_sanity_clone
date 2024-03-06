from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, JDBC_DATABASE: str=None, **kwargs):
        self.spark = None
        self.update(JDBC_DATABASE)

    def update(self, JDBC_DATABASE: str="test_database", **kwargs):
        prophecy_spark = self.spark
        self.JDBC_DATABASE = JDBC_DATABASE
        pass
