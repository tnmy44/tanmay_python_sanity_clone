from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight, computeTargetName, SchemaFields, getColumnsInSchema, \
    getColumnsToHighlight2, validateSColumn, sanitizedColumn, ColumnsUsage
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class OrderByRule:
    expression: SColumn
    sortType: str


class OrderBy(ComponentSpec):
    name: str = "CustomOrderByMainTransformCategory"
    category: str = "Transform"
    gemDescription: str = "Sort a DataFrame on one or more columns in ascending or descending order."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/order-by"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class OrderByProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        orders: List[OrderByRule] = field(default_factory=list)

    def onClickFunc(self, portId: str, column: str, state: Component[OrderByProperties]):
        orders = state.properties.orders
        orders.append(OrderByRule(SColumn.getSColumn(sanitizedColumn(column)), "asc"))
        return state.bindProperties(replace(state.properties, orders=orders))

    def allColumnsSelectionFunc(self, portId: str, state: Component[OrderByProperties]):
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        orders = list(map(lambda column: OrderByRule(SColumn.getSColumn(sanitizedColumn(column)), "asc"), columnsInSchema))
        updatedOrders = (state.properties.orders)
        updatedOrders.extend(orders)
        return state.bindProperties(replace(state.properties, orders=updatedOrders))

    def dialog(self) -> Dialog:
        return Dialog("OrderBy").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty=("columnsSelector"),
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc
                ).importSchema(),
                "2fr"
            )
                .addColumn(
                BasicTable("OrderByTable", columns=[Column(
                    "Order Columns",
                    "expression.expression",
                    (
                        ExpressionBox(ignoreTitle=True)
                            .bindPlaceholders()
                            .bindLanguage("${record.expression.format}")
                            .withSchemaSuggestions()
                    )
                ),
                    Column("Sort", "sortType", SelectBox("")
                           .addOption("Ascending", "asc")
                           .addOption("Descending", "desc"), width="25%")
                ])
                    .bindProperty("orders"),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[OrderByProperties]) -> List[Diagnostic]:
        diagnostics = []
        if len(component.properties.orders) == 0:
            diagnostics.append(
                Diagnostic("properties.orders", "At least one order rule has to be specified", SeverityLevelEnum.Error))
        else:
            for idx, expr in enumerate(component.properties.orders):
                d1 = validateSColumn(expr.expression, f"orders[{idx}].expression", component,
                                     ColumnsUsage.WithoutInputAlias)
                diagnostics.extend(d1)
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[OrderByProperties], newState: Component[OrderByProperties]) -> \
            Component[
                OrderByProperties]:
        newProps = newState.properties
        usedColExps = getColumnsToHighlight2([x.expression for x in newProps.orders], newState)
        orders = newState.properties.orders
        orders = [x if not x.sortType == "" else OrderByRule(SColumn.getSColumn(x.expression.rawExpression), "asc") for
                  x in orders]
        return newState.bindProperties(replace(newProps, columnsSelector=usedColExps, orders=orders))

    class OrderByCode(ComponentCode):
        def __init__(self, newProps):
            self.props: OrderBy.OrderByProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            orderRules = map(lambda x:
                             x.expression.column().asc() if (x.sortType == "asc") else x.expression.column().desc(),
                             self.props.orders)

            return in0.orderBy(*orderRules)
