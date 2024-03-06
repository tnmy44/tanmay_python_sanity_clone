from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from pythonbasic.test.mainone.graph.Reformat_3 import *
from pythonbasic.test.mainone.config.ConfigStore import *


class Reformat_3Test(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_2(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_2.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_2.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_3(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_3.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_3.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_4(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_4.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_4.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_5(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_5.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_5.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)

    def test_unit_test_6(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_6.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_6.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c  date"), dfOutComputed.select("c  date"), self.maxUnequalRowsToShow)

    def test_unit_test_7(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_7.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_7.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c  date"), dfOutComputed.select("c  date"), self.maxUnequalRowsToShow)

    def test_unit_test_8(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_8.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_8.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c  date"), dfOutComputed.select("c  date"), self.maxUnequalRowsToShow)

    def test_unit_test_9(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/in0/data/test_unit_test_9.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/schema.json',
            'test/resources/data/pythonbasic/test/mainone/graph/Reformat_3/out/data/test_unit_test_9.json',
            'out'
        )
        dfOutComputed = Reformat_3(self.spark, dfIn0)
        assertDFEquals(dfOut.select("c  date"), dfOutComputed.select("c  date"), self.maxUnequalRowsToShow)

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
