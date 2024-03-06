from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from pythonbasic.test.mainone.graph.reformat_columns import *
from pythonbasic.test.mainone.config.ConfigStore import *


class reformat_columnsTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = reformat_columns(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "`c___-- string`",
              "c  date",
              "c_timestamp"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "`c___-- string`",
              "c  date",
              "c_timestamp"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/reformat_columns/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = reformat_columns(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "`c___-- string`",
              "c  date",
              "c_timestamp"
            ),
            dfOutComputed.select(
              "c   short  --",
              "c-int-column type",
              "-- c-long",
              "c-decimal",
              "c  float",
              "c--boolean",
              "c- - -double",
              "`c___-- string`",
              "c  date",
              "c_timestamp"
            ),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
