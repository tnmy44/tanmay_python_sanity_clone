import pendulum
from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base.datatypes import SInt
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base import WorkflowContext


class Limit(ComponentSpec):
    name: str = "DepLimit"
    category: str = "Transform"
    gemDescription: str = "Limits the number of rows in the output"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/transform/limit/"

    def optimizeCode(self) -> bool:
        return True

    def dependency_method(self):
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        from gensim.models import Word2Vec
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return model

    @dataclass(frozen=True)
    class LimitProperties(ComponentProperties):
        limit: SInt = SInt("10")

    def dialog(self) -> Dialog:
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        from gensim.models import Word2Vec
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
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
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        from gensim.models import Word2Vec
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
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
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        from gensim.models import Word2Vec
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return newState


    class LimitCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Limit.LimitProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pendulum
            now_in_paris = pendulum.now('Europe/Paris')
            past = pendulum.now().subtract(minutes=2)
            print(now_in_paris)
            print(past.diff_for_humans())
            from gensim.models import Word2Vec
            sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
            model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
            return in0.limit(self.props.limit.value)
