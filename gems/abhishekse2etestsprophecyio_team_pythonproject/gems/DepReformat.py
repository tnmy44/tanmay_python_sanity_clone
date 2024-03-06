from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *
import re
import pendulum
from gensim.models import Word2Vec

from prophecy.cb.ui.UISpecUtil import getColumnsToHighlight, computeTargetName, SchemaFields, getColumnsInSchema, \
    validateExpTable, ColumnsUsage, getTargetTokens, sanitizedColumn
from prophecy.cb.util.CSVUtils import parse_escaped_csv, unparse_escaped_csv, CSVParseException
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext

class Reformat(ComponentSpec):
    name: str = "DepReformat"
    category: str = "Transform"
    gemDescription: str = "Edits column names or values using expressions."
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/reformat/"

    def optimizeCode(self) -> bool:
        return True

    def dependency_method(self):
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return model

    @dataclass(frozen=True)
    class ReformatProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        expressions: List[SColumnExpression] = field(default_factory=list)
        activeTab: str = "expressions"
        importLanguage: str = "${$.workflow.metainfo.frontEndLanguage}"
        importString: str = ""

    def onClickFunc(self, portId: str, column: str, state: Component[ReformatProperties]):
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        existingTargetNames = list(map(lambda exp: exp.target, state.properties.expressions))
        targetTokens = getTargetTokens(column, [exp.split('.') for exp in existingTargetNames], True)
        targetCol = '.'.join(targetTokens)
        expressions = state.properties.expressions
        expressions.append(SColumnExpression(targetCol, SColumn.getSColumn(sanitizedColumn(column)), ""))
        return state.bindProperties(replace(state.properties, expressions=expressions))

    def allColumnsSelectionFunc(self, portId: str, state: Component[ReformatProperties]):
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        columnsInSchema = getColumnsInSchema(portId, state, SchemaFields.TopLevel)
        expressions = list(
            map(lambda column: SColumnExpression.getSColumnExpression(sanitizedColumn(column)), columnsInSchema))
        state.properties.expressions.extend(expressions)
        return state.bindProperties(replace(state.properties, expressions=state.properties.expressions))

    def expressions_to_csv(self, state: Component[ReformatProperties]) -> Component[ReformatProperties]:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        exprCSV = []
        for e in state.properties.expressions:
            linepart = [e.target, e.expression.rawExpression]
            if len(e.description):
                linepart.append(e.description)
            exprCSV.append(linepart)
        csv_string = unparse_escaped_csv(exprCSV)
        return state.bindProperties(replace(state.properties, importString=csv_string))

    def csv_to_expressions(self, state: Component[ReformatProperties]) -> Component[ReformatProperties]:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        exprs = []
        # for (target, exp, desc) in parse_escaped_csv(state.properties.importString):
        for line in parse_escaped_csv(state.properties.importString, field_min=2, field_max=2):
            (target, exp) = line
            exprs.append(SColumnExpression(
                target.strip(),
                SColumn(
                    exp,
                    state.properties.importLanguage,
                    col(exp),
                    [exp]
                ),
                ""
            ))
        return state.bindProperties(replace(state.properties, expressions=exprs))

    def dialog(self) -> Dialog:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        expTable = ExpTable("Reformat Expression") \
            .enableVirtualization() \
            .bindProperty("expressions") \
            .withCopilotEnabledExpressions(
            CopilotSpec(
                method="copilot/getExpression",
                methodType="CopilotProjectionExpressionRequest",
                copilotProps=CopilotPromptTypeProps(
                    buttonLabel="Ask AI",
                )
            )
        )
        bulkEdit = StackLayout(height="100%") \
            .addElement(
            NativeText("Edit the Reformat expressions in the field below. Use the format of \"name,expr\".")
        ).addElement(
            NativeText("Use ``...`` to wrap multi-line expressions.")
        ).addElement(
            Editor(height="100%", language="${component.properties.importLanguage}") \
                .bindProperty("importString")
        )
        refTabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(
            TabPane("Expressions", "expressions").addElement(expTable)
        ).addTabPane(
            TabPane("Advanced", "advanced").addElement(bulkEdit)
        )
        return Dialog("Reformat").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                PortSchemaTabs(
                    allowInportRename=True,
                    selectedFieldsProperty="columnsSelector",
                    singleColumnClickCallback=self.onClickFunc,
                    allColumnsSelectionCallback=self.allColumnsSelectionFunc
                ).importSchema(),
                "2fr"
            )
                .addColumn(refTabs, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[ReformatProperties]) -> List[Diagnostic]:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        diagnostics = []
        expTableDiags = validateExpTable(component.properties.expressions, "expressions", component,
                                         ColumnsUsage.WithoutInputAlias)
        diagnostics.extend(expTableDiags)

        if component.properties.activeTab == "advanced":
            try:
                parse_escaped_csv(component.properties.importString, field_min=2, field_max=3)
            except CSVParseException as e:
                diagnostics.append(Diagnostic(f"properties.importString", str(e), SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[ReformatProperties], newState: Component[ReformatProperties]) -> Component[
        ReformatProperties]:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        oldProps = oldState.properties
        newProps = newState.properties

        if oldProps.activeTab == "advanced" and newProps.activeTab == "expressions":
            try:
                newState = self.csv_to_expressions(newState)
                newProps = newState.properties

            except CSVParseException:
                pass
        elif oldProps.activeTab == "expressions" and newProps.activeTab == "advanced":
            newState = self.expressions_to_csv(newState)
            newProps = newState.properties

        expressions = newProps.expressions

        usedColExps = getColumnsToHighlight(expressions, newState)

        return newState.bindProperties(replace(newProps,
                                               columnsSelector=usedColExps,
                                               expressions=list(
                                                   map(lambda exp: exp.withRowId(), expressions))))

    def getCPStmt(self, component: Component[ReformatProperties]) -> Optional[SelectStmt]:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return SelectStmt(
            component.component,
            [ProjectionExpression(expression.target, expression.expression.rawExpression) for expression in
             component.properties.expressions]
        )

    class ReformatCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Reformat.ReformatProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pendulum
            from gensim.models import Word2Vec
            now_in_paris = pendulum.now('Europe/Paris')
            past = pendulum.now().subtract(minutes=2)
            print(now_in_paris)
            print(past.diff_for_humans())
            sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
            model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
            if len(self.props.expressions) > 0:
                selectColumns = map(lambda x: x.column(), self.props.expressions)
                return in0.select(*selectColumns)
            else:
                return in0
