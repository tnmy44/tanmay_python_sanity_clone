from pyspark.sql import *

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.server.base.datatypes import SString
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

@dataclass(frozen=True)
class FileTab:
    path: str
    id: str
    language: str
    content: SString


class SQLStatement(ComponentSpec):
    name: str = "CustomSQLStatementMainCustomCategory"
    category: str = "Custom"
    gemDescription: str = "Creates one or more DataFrame(s) based on provided SQL queries to run against one or more input DataFrame(s)."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/custom/sql-statement"


    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SQLStatementProperties(ComponentProperties):
        fileTabs: List[FileTab] = field(
            default_factory=lambda: [FileTab(path="out", id="out", language="sql", content=SString(""))])
        inputPortNames: List[str] = field(default_factory=lambda: ["in0"])

    def dialog(self) -> Dialog:
        placeholders = {
            "scala": """select * from in0""",
            "python": """select * from in0""",
            "sql": """select * from in0"""}

        return Dialog("SQLStatement").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                PortSchemaTabs(
                    editableInput=True,
                    allowOutportRename=True,
                    allowOutportAddDelete=True
                ).importSchema()
            )
                .addColumn(
                FileEditor(newFilePrefix="out", newFileLanguage="sql", minFiles=1)
                    .withSchemaSuggestions()
                    .bindPlaceholders(placeholders)
                    .bindProperty("fileTabs"),
                "2fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[SQLStatementProperties]) -> List[Diagnostic]:
        diagnostics = []
        if (len(component.ports.outputs) != len(component.properties.fileTabs)):
            diagnostics.append(Diagnostic(
                "properties.fileTabs",
                "Number of output ports have to be the same as the number of file tabs.",
                SeverityLevelEnum.Error
            ))

        for idx, fileTab in enumerate(component.properties.fileTabs):

            if fileTab.content.diagnosticMessages is not None:
                for message in fileTab.content.diagnosticMessages:
                    diagnostics.append(Diagnostic(
                        f"properties.fileTabs[{idx}]", f"{fileTab.path} : {message}", SeverityLevelEnum.Error
                    ))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[SQLStatementProperties], newState: Component[SQLStatementProperties]) -> \
            Component[SQLStatementProperties]:
        currentTabs = newState.properties.fileTabs
        revisedFileTabs = []
        revisedInputPortNames = []
        for port in newState.ports.outputs:
            language = currentTabs[0].language
            matchingTab = [tab for tab in currentTabs if (tab.path == port.slug)]
            newFileTab = FileTab(port.slug, port.id, language, "")
            if len(matchingTab) != 0:
                newFileTab = replace(newFileTab, content=matchingTab[0].content)
            revisedFileTabs.append(newFileTab)

        for port in newState.ports.inputs:
            revisedInputPortNames.append(port.slug)

        updatedProperties = replace(newState.properties, fileTabs=revisedFileTabs,
                                    inputPortNames=revisedInputPortNames)
        return replace(newState, properties=updatedProperties)

    class SQLStatementCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SQLStatement.SQLStatementProperties = newProps

        def apply(self, spark: SparkSession, *inDFs: DataFrame) -> (DataFrame, List[DataFrame]):
            outport = 0
            try:
                registerUDFs(spark)
            except NameError:
                print("registerUDFs not working")
            for tempInDF in inDFs:
                tempInDF.createOrReplaceTempView(self.props.inputPortNames[outport])
                outport = outport + 1

            output = []

            for fileTab in self.props.fileTabs:
                output.append(spark.sql(fileTab.content.value))
            return output[0], output[1:]
