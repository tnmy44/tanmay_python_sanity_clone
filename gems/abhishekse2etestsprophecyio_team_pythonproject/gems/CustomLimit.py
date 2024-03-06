from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.datatypes import SInt
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext

class Limit(ComponentSpec):
    name: str = "CustomLimit"
    category: str = "CustomTransform"
    gemDescription: str = "Limits the number of rows in the output"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/limit/"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class LimitProperties(ComponentProperties):
        limit: SInt = SInt("10")

    def dialog(self) -> Dialog:
        return Dialog("Limit").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "2fr")
                .addColumn(
                ExpressionBox("Limit")
                    .bindPlaceholder("10")
                    .bindProperty("limit")
                    .withFrontEndLanguage(),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[LimitProperties]) -> List[Diagnostic]:
        diagnostics = []
        limitDiagMsg = "Limit has to be an integer between [0, (2**31)-1]"
        if component.properties.limit.diagnosticMessages is not None and len(component.properties.limit.diagnosticMessages) > 0:
            for message in component.properties.limit.diagnosticMessages:
                diagnostics.append(Diagnostic("properties.limit", message, SeverityLevelEnum.Error))
        else:
            resolved = component.properties.limit.value
            if resolved <= 0:
                diagnostics.append(Diagnostic("properties.limit", limitDiagMsg, SeverityLevelEnum.Error))
            else:
                pass
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[LimitProperties], newState: Component[LimitProperties]) -> Component[
        LimitProperties]:
        return newState


    class LimitCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Limit.LimitProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            return in0.limit(self.props.limit.value)
