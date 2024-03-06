from typing import List, Any

from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight2, InputPortSchema
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class SetOperation(ComponentSpec):
    name: str = "CustomSetOperation"
    category: str = "CustomTransform"
    gemDescription: str = "Allows you to perform Unions and Intersections of records from DataFrames with similar schemas and different data."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/set-operation"

    def optimizeCode(self) -> bool:
        return True
    def allInputsRequired(self) -> bool:
        return False

    @dataclass(frozen=True)
    class SetOperationProperties(ComponentProperties):
        operationType: str = "unionAll"
        allowMissingColumns: Optional[bool] = False

    def dialog(self) -> Dialog:
        return Dialog("SetOperation").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
            .addColumn(
                PortSchemaTabs(editableInput=True,
                               minNumberOfPorts=2).importSchema()
            )
            .addColumn(
                StackLayout()
                .addElement(
                    RadioGroup("Operation Type")
                    .addOption(
                        "Union",
                        "unionAll",
                        ("UnionAll"),
                        ("Returns a dataset containing rows in any one of the input Datasets, while preserving duplicates.")
                    )
                    .addOption(
                        "Intersect All",
                        "intersectAll",
                        ("IntersectAll"),
                        ("Returns a dataset containing rows in all of the input Datasets, while preserving duplicates.")
                    )
                    .addOption(
                        "Except All",
                        "exceptAll",
                        ("ExceptAll"),
                        (
                            "Returns a dataset containing rows in the first Dataset, but not in the other datasets, while preserving duplicates.")
                    )
                    .addOption(
                        "Union By Name",
                        "unionByName",
                        ("UnionAll"),
                        ("Returns a dataset containing rows in any one of the input Datasets, while preserving duplicates merging them by the column names instead of merging them by position")
                    )
                    .setOptionType("button")
                    .setVariant("large")
                    .setButtonStyle("solid")
                    .bindProperty("operationType"))
                .addElement(
                    Checkbox("Allow missing columns").bindProperty(
                        "allowMissingColumns")
                ),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[SetOperationProperties]) -> List[Diagnostic]:
        from collections import OrderedDict

        def getDiagnostic(errorMessage: str) -> List[Diagnostic]:
            return [Diagnostic("ports.inputs", errorMessage, SeverityLevelEnum.Error)]

        def mismatchedNumberOfColumnsError(
                anomalous: OrderedDict,
                baseline: OrderedDict,
                anomalousPortSlug: str
        ):
            missingInAnomalous = OrderedDict(baseline)
            for key in anomalous.keys():
                if key in missingInAnomalous.keys():
                    del missingInAnomalous[key]
            m1 = f"""Columns {", ".join(missingInAnomalous.keys())} not found in {anomalousPortSlug}. """ if (
                len(missingInAnomalous)) else ""

            extraInAnomalous = OrderedDict(anomalous)
            for key in baseline.keys():
                if key in extraInAnomalous:
                    del extraInAnomalous[key]

            m2 = f"""Additional columns {", ".join(extraInAnomalous.keys())} found in {anomalousPortSlug}.""" if (
                len(extraInAnomalous)) else ""

            return f"Number of columns should be the same. {m1} {m2}"

        def mismatchedColNameOrDatatypeError(
                anomalous: OrderedDict,
                baseline: OrderedDict,
                anomalousPortSlug: str
        ):
            diffs = []
            for anomalousKeyValue, baselineKeyValue in zip(anomalous.items(), baseline.items()):
                anomalousKey, anomalousValue = anomalousKeyValue[0], anomalousKeyValue[1]
                baselineKey, baselineValue = baselineKeyValue[0], baselineKeyValue[1]
                if anomalousKey != baselineKey:
                    diffs.append(
                        f"{anomalousKey}({anomalousValue})/{baselineKey}({baselineValue})")
            if len(diffs) > 0:
                return f"""Mismatch in columns of "{anomalousPortSlug}" port. {". ".join(diffs)}"""
            else:
                return ""

        def getAnomalousSchema(schemasGrouped: List[List[InputPortSchema]]) -> (
                str, OrderedDict, OrderedDict):
            (firstGroupRep, secondGroupRep, secondGroupNumItems) = (
                schemasGrouped[0][0], schemasGrouped[-1][0], len(schemasGrouped[-1]))
            if (secondGroupNumItems == 1):
                return (secondGroupRep.slug, secondGroupRep.schemaSummary, firstGroupRep.schemaSummary)
            else:
                return (firstGroupRep.slug, firstGroupRep.schemaSummary, secondGroupRep.schemaSummary)

        def foo(schema: StructType, port: NodePort):
            testMap = OrderedDict()
            foo1 = [(field.name, field.dataType) for field in schema.fields]
            for x in foo1:
                testMap[x[0]] = x[1]
            return InputPortSchema(port.slug, testMap)

        # Spark allows column order to be differet is using unionByName it also allows missing columns if allowMissingColumns is set.
        # Also conversion to super types is support for example null (indicating missing values) to string type it also allows int to string type conversion.
        # Currently we should atleast allow null to other types conversion as null indicates missing value and it is valid use case for customers.
        def supersetForInputSchema(schemaList: list[InputPortSchema]):
            if component.properties.operationType != "unionByName":
                return schemaList

            allColumns = []
            for inputPortSchema in schemaList:
                allColumns += [k for k in inputPortSchema.schemaSummary]

            # For schema analysis add missing columns as null
            if component.properties.allowMissingColumns:
                for index in range(len(schemaList)):
                    for column in allColumns:
                        if column not in schemaList[index].schemaSummary:
                            schemaList[index].schemaSummary[column] = NullType()
            # Sort keys if union by name.
            schemaList = [InputPortSchema(schema.slug, OrderedDict(
                sorted(schema.schemaSummary.items()))) for schema in schemaList]

            # Tries to find super type of two type, if possible will return (True, SuperType) else (False, None).
            def typeCast(first: DataType, second: DataType) -> (bool, DataType):
                if type(first) ==  type(second):
                    return (True, first)
                elif type(first) ==  type(NullType()):
                    return (True, second)
                elif type(second) ==  type(NullType()):
                    return (True, first)
                else:
                    return (False, None)

            # Type cast if possible.
            resolvedTypes = {}
            newSchemaList = []
            for inputPortSchema in schemaList:
                for column in inputPortSchema.schemaSummary:
                    typeResolutionResult = typeCast(resolvedTypes.get(
                        column, NullType()), inputPortSchema.schemaSummary[column])
                    if typeResolutionResult[0] is False:
                        return schemaList
                    resolvedTypes[column] = typeResolutionResult[1]
            for inputPortSchema in schemaList:
                schema = OrderedDict()
                for column in inputPortSchema.schemaSummary:
                    schema[column] = resolvedTypes[column]
                newSchemaList += [InputPortSchema(
                    inputPortSchema.slug, schema)]
            return newSchemaList

        if (len(component.ports.inputs) < 2):
            return getDiagnostic("Input ports can't be less than two in SetOperation")
        else:
            # Each array in the list represents schema of one input port. Contains the fieldname and field datatype of that schema.
            tmp: list[InputPortSchema] = supersetForInputSchema(
                [foo(port.schema, port) for port in component.ports.inputs])

            if len(tmp) == 0:
                schemaNamesAndDataTypes: List[InputPortSchema] = List[InputPortSchema(
                    "", OrderedDict())]
            else:
                schemaNamesAndDataTypes: List[InputPortSchema] = tmp

            # grouping by keyset (Set) for groups schemas with shuffled columns in same group.
            # Converting the keyset to a list ensures the positions of the keys becomes static
            # and case of shuffled columns does not break

            # schemaNamesAndDataTypes = [InputPortSchema("in0", OrderedDict(
            #     {"customer_id": "string", "first_name": "string", "last_name": "string", "age": "integer",
            #      "location": "string"})), InputPortSchema("in0", OrderedDict(
            #     {"customer_id": "integer", "first_name": "string", "last_name": "string", "age": "integer",
            #      "location": "string"}))]
            from itertools import groupby

            groups = []
            uniquekeys = []
            for k, g in groupby(schemaNamesAndDataTypes, lambda x: [x.schemaSummary]):
                groups.append(list(g))
                uniquekeys.append(k)

            # schemasGrouped: OrderedDict[List[str], List[InputPortSchema]] =
            # CASE: perfect scenario. number of columns match and their data types also match
            if (len(uniquekeys) == 1):
                return []
            # CASE if two groups are created, the reason might have either been
            # 1. diff num of columns or 2. same num of columns but diff colnames or datatypes
            # We'll give a clean diagnostics if we have only one port in such an error state. Generic otherwise.
            elif (len(uniquekeys) == 2 and (any([len(x) == 1 for x in groups]))):
                (anomalousPortSlug, anomalous, baseline) = getAnomalousSchema(groups)
                # different number of columns
                if len(set([len(keySet[0]) for keySet in uniquekeys])) == 2:
                    return getDiagnostic(mismatchedNumberOfColumnsError(anomalous, baseline, anomalousPortSlug))
                else:  # same number of columns but diff colname and datatype
                    errormsg = mismatchedColNameOrDatatypeError(anomalous, baseline, anomalousPortSlug)
                    if errormsg != "":
                        return getDiagnostic(mismatchedColNameOrDatatypeError(anomalous, baseline, anomalousPortSlug))
                    else:
                        return []
            else:
                return getDiagnostic("Mismatch in columns found. All inputs must have the same columns.")

    def onChange(self, context: WorkflowContext, oldState: Component[SetOperationProperties], newState: Component[SetOperationProperties]) -> \
            Component[SetOperationProperties]:
        return newState

    class SetOperationCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SetOperation.SetOperationProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame, in1: DataFrame, *inDFs: DataFrame) -> DataFrame:
            _inputs = [in0, in1]
            _inputs.extend(inDFs)

            nonEmptyDf: SubstituteDisabled = [x for x in _inputs if x is not None]
            res: SubstituteDisabled = nonEmptyDf[0]
            rest: SubstituteDisabled = nonEmptyDf[1:]

            if self.props.operationType == "intersectAll":
                for inDF in rest:
                    res = res.intersectAll(inDF)
            elif self.props.operationType == "exceptAll":
                for inDF in rest:
                    res = res.exceptAll(inDF)
            elif self.props.operationType == "unionAll":
                for inDF in rest:
                    res = res.unionAll(inDF)
            elif self.props.operationType == "unionByName":
                if self.props.allowMissingColumns:
                    for inDF in rest:
                        res = res.unionByName(inDF, allowMissingColumns=True)
                else:
                    for inDF in rest:
                        res = res.unionByName(inDF)
            return res
