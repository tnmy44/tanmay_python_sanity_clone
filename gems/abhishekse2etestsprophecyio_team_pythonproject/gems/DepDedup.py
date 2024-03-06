from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.ui.UISpecUtil import (
    getColumnsToHighlight3,
    getColumnsInSchema,
    SchemaFields,
    ColumnsUsage,
    validateSColumn,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext
import pendulum
from gensim.models import Word2Vec

@dataclass(frozen=True)
class StringColName:
    colName: str


@dataclass(frozen=True)
class OrderByRule:
    expression: SColumn
    sortType: str


class Deduplicate(ComponentSpec):
    name: str = "DepDedup"
    category: str = "CustomTransform"
    gemDescription: str = "Removes rows with duplicate values of specified columns."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/deduplicate"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class DeduplicateProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        dedupType: str = "any"
        dedupColumns: List[StringColName] = field(default_factory=list)
        useOrderBy: bool = False
        orders: List[OrderByRule] = field(default_factory=list)

    def onClickFunc(
            self, portId: str, column: str, state: Component[DeduplicateProperties]
    ):
        dedups = state.properties.dedupColumns
        dedups.append(StringColName(column))
        return state.bindProperties(replace(state.properties, dedupColumns=dedups))

    def allColumnsSelectionFunc(
            self, portId: str, state: Component[DeduplicateProperties]
    ):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        dedups = list(map(lambda column: (StringColName(column)), columnsInSchema))
        updatedDedups = state.properties.dedupColumns
        updatedDedups.extend(dedups)
        return state.bindProperties(
            replace(state.properties, dedupColumns=updatedDedups)
        )

    def dialog(self) -> Dialog:

        orderByTable = BasicTable(
            "OrderByTable",
            height="300px",
            columns=[
                Column(
                    "Order Column Names",
                    "expression.expression",
                    (
                        ExpressionBox(ignoreTitle=True)
                            .bindPlaceholders()
                            .bindLanguage("${record.expression.format}")
                            .withSchemaSuggestions()
                    ),
                ),
                Column(
                    "Sort",
                    "sortType",
                    SelectBox("")
                        .addOption("Ascending", "asc")
                        .addOption("Descending", "desc"),
                    width="25%",
                        ),
            ],
        ).bindProperty("orders")

        selectBox = (
            SelectBox("Row to keep little buddy")
                .addOption("Distinct Rows Only", "distinct")
                .addOption("Unique Rows Only", "unique_only")
                .addOption("Last Rows", "last")
                .addOption("First Rows", "first")
                .addOption("Any Rows", "any")
                .bindProperty("dedupType")
        )
        testTable = BasicTable(
            "Test",
            height="300px",
            columns=[
                Column(
                    "Deduplicate Columns",
                    "colName",
                    (TextBox("", ignoreTitle=True).disabled()),
                )
            ],
        ).bindProperty("dedupColumns")

        return Dialog("Deduplicate").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty=("columnsSelector"),
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc,
                ).importSchema()
            )
                .addColumn(
                StackLayout(height=("100%"))
                    .addElement(selectBox)
                    .addElement(
                    Condition().ifNotEqual(PropExpr("component.properties.dedupType"), StringExpr("distinct"))
                        .then(testTable)
                )
                    .addElement(
                    Condition()
                        .ifEqual(
                        PropExpr("component.properties.dedupType"), StringExpr("first")
                    )
                        .then(
                        StackLayout()
                            .addElement(
                            Checkbox("Use Custom OrderBy").bindProperty("useOrderBy")
                        )
                            .addElement(
                            Condition()
                                .ifEqual(
                                PropExpr("component.properties.useOrderBy"),
                                BooleanExpr(True),
                            )
                                .then(orderByTable)
                        )
                    )
                        .otherwise(
                        Condition()
                            .ifEqual(
                            PropExpr("component.properties.dedupType"),
                            StringExpr("last"),
                        )
                            .then(
                            StackLayout(height=("100%"))
                                .addElement(
                                Checkbox("Use Custom OrderBy").bindProperty(
                                    "useOrderBy"
                                )
                            )
                                .addElement(
                                Condition()
                                    .ifEqual(
                                    PropExpr("component.properties.useOrderBy"),
                                    BooleanExpr(True),
                                )
                                    .then(orderByTable)
                            )
                        )
                    )
                ),
                "2fr",
                    )
        )

    def validate(self, context: WorkflowContext, component: Component[DeduplicateProperties]) -> List[Diagnostic]:
        diagnostics = []
        if component.properties.dedupType=="distinct":
            return diagnostics
        if len(component.properties.dedupColumns) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.dedupColumns",
                    "At least one rule has to be specified",
                    SeverityLevelEnum.Error,
                )
            )
        else:
            for idx, col in enumerate(component.properties.dedupColumns):

                if "." in col.colName:
                    diagnostics.append(
                        Diagnostic(
                            f"properties.dedupColumns[{idx}].colName",
                            "Deduplication can not be done for nested cols flatten your schema before deduplicating using a nested column.",
                            SeverityLevelEnum.Error,
                        )
                    )

        if component.properties.useOrderBy:
            if len(component.properties.orders) == 0:
                diagnostics.append(
                    Diagnostic(
                        "properties.orders",
                        "At least 1 rule to be specified",
                        SeverityLevelEnum.Error,
                    )
                )
            else:
                for idx, expr in enumerate(component.properties.orders):
                    d1 = validateSColumn(
                        expr.expression,
                        f"orders[{idx}].expression",
                        component,
                        ColumnsUsage.WithoutInputAlias,
                    )
                    diagnostics.extend(d1)
        return diagnostics

    def onChange(
            self,
            context: WorkflowContext,
            oldState: Component[DeduplicateProperties],
            newState: Component[DeduplicateProperties],
    ) -> Component[DeduplicateProperties]:
        newProps = newState.properties
        usedColExps = getColumnsToHighlight3(
            [x.colName for x in newProps.dedupColumns], newState
        )
        return newState.bindProperties(
            replace(
                newProps, columnsSelector=usedColExps, useOrderBy=newProps.useOrderBy
            )
        )

    class DeduplicateCode(ComponentCode):
        """
        Method for Deduplicate operation when rows to be kept in each group of rows to be either first, Last or
        unique-only. It does first groupBy on all passed groupByColumns and then depending on typeToKeep value it
        does further operations.
        For both first and last option, it adds new temporary row_number column which returns the row number within a
        group of rows grouped by groupByColumns. Then to find first records it simply filters out all rows with
        row_number as 1. To find last records within each group it also computes the count value for each group
        and filters out all the records where row_number is same as group count
        For unique-only case it adds new temporary count column which returns the count of rows within a window
        partition. Then it filters the resultant dataframe with count value 1.
        @param typeToKeep     option to find kind of rows. Possible values are first, last and unique-only
        @param groupByColumns columns to be used to group input records.
        @return DataFrame with first or last or unique-only records in each grouping of input records.
        :rtype: object
        """

        def __init__(self, newProps):
            self.props: Deduplicate.DeduplicateProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pendulum
            from gensim.models import Word2Vec
            now_in_paris = pendulum.now('Europe/Paris')
            past = pendulum.now().subtract(minutes=2)
            print(now_in_paris)
            print(past.diff_for_humans())
            sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
            model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
            typeToKeep = self.props.dedupType
            groupByColumns = list(map(lambda x: x.colName, self.props.dedupColumns))

            orderRules = [lit(1)]
            if self.props.useOrderBy:
                orderRules = map(
                    lambda x: x.expression.column().asc()
                    if (x.sortType == "asc")
                    else x.expression.column().desc(),
                    self.props.orders,
                )

            window = Window.partitionBy(*groupByColumns) \
                .orderBy(*orderRules)

            windowForCount = Window.partitionBy(*groupByColumns)

            if typeToKeep == "any":
                if len(groupByColumns) == 0:
                    columns = in0.columns
                else:
                    columns = groupByColumns
                return in0.dropDuplicates(columns)
            elif typeToKeep == "distinct":
                return in0.distinct()
            elif typeToKeep == "first":
                dataFrameWithRowNumber = in0.withColumn(
                    "row_number", row_number().over(window)
                )
                return dataFrameWithRowNumber.filter(col("row_number") == lit(1)).drop(
                    "row_number"
                )

            elif typeToKeep == "last":
                dataFrameWithRowNumber = in0.withColumn(
                    "row_number", row_number().over(window)
                ).withColumn("count", count("*").over(windowForCount))
                return dataFrameWithRowNumber.filter(col("row_number") == col("count")) \
                    .drop("row_number") \
                    .drop("count")

            elif typeToKeep == "unique_only":
                dataFrameWithCount = in0.withColumn(
                    "count", count("*").over(windowForCount)
                )
                return dataFrameWithCount.filter(col("count") == lit(1)).drop("count")
