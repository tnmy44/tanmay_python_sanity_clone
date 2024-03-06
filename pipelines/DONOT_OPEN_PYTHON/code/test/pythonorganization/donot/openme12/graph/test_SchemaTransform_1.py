from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from pythonorganization.donot.openme12.graph.SchemaTransform_1 import *
from pythonorganization.donot.openme12.config.ConfigStore import *


class SchemaTransform_1Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/SchemaTransform_1/in0/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/SchemaTransform_1/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/SchemaTransform_1/out/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/SchemaTransform_1/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = SchemaTransform_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("customer_id", "first_name", "last_name", "email_main", "country_code", "full_name"),
            dfOutComputed.select("customer_id", "first_name", "last_name", "email_main", "country_code", "full_name"),
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
        dfdonot_openme12_graph_Lookup_1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/pythonorganization/donot/openme12/graph/Lookup_1/schema.json',
            'test/resources/data/pythonorganization/donot/openme12/graph/Lookup_1/data.json',
            "in0"
        )
        from pythonorganization.donot.openme12.graph.Lookup_1 import Lookup_1
        Lookup_1(self.spark, dfdonot_openme12_graph_Lookup_1)
