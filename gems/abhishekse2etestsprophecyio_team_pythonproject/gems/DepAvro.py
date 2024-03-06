from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from prophecy.cb.server.base import WorkflowContext
import pendulum


class avro(DatasetSpec):
    name: str = "DepAvro"
    datasetType: str = "File"
    mode: str = "batch"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/avro"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class AvroProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        useSchema: Optional[bool] = False
        path: str = ""
        ignoreExtension: Optional[bool] = None
        avroschema: Optional[str] = None
        writeMode: Optional[str] = None
        compression: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        recordName: Optional[str] = None
        recordNamespace: Optional[str] = None
        pathGlobFilter: Optional[str] = None
        modifiedBefore: Optional[str] = None
        modifiedAfter: Optional[str] = None
        recursiveFileLookup: Optional[bool] = None

    def dependency_method(self):
        from gensim.models import Word2Vec
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return model

    def sourceDialog(self) -> DatasetDialog:
        from gensim.models import Word2Vec
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return DatasetDialog("avro") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description...").withCopilot(
                                    copilot=CopilotSpec(
                                        method="copilot/describe",
                                        methodType="CopilotDescribeDataSourceRequest",
                                        copilotProps=CopilotButtonTypeProps(
                                            buttonLabel="Auto-description",
                                            align="end",
                                            gap=4
                                        )
                                    )
                                ),
                                "description",
                                True
                            )
                                .addField(Checkbox("Use user-defined schema"), "useSchema", True)
                                .addField(Checkbox("Ignore files without .avro extension while reading"),
                                          "ignoreExtension")
                                .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                                .addField(TextBox("Path Global Filter").bindPlaceholder(""), "pathGlobFilter")
                                .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore")
                                .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter")
                                .addField(TextBox("Avro Schema").bindPlaceholder("json schema"), "avroschema")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")
        )

    def targetDialog(self) -> DatasetDialog:
        from gensim.models import Word2Vec
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return DatasetDialog("avro") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description...").withCopilot(
                                    copilot=CopilotSpec(
                                        method="copilot/describe",
                                        methodType="CopilotDescribeDataSourceRequest",
                                        copilotProps=CopilotButtonTypeProps(
                                            buttonLabel="Auto-description",
                                            align="end",
                                            gap=4
                                        )
                                    )
                                ),
                                "description",
                                True
                            )
                                .addField(
                                SelectBox("Write Mode")
                                    .addOption("error", "error")
                                    .addOption("overwrite", "overwrite")
                                    .addOption("append", "append")
                                    .addOption("ignore", "ignore"),
                                "writeMode"
                            )
                                .addField(
                                SelectBox("Compression")
                                    .addOption("uncompressed", "uncompressed")
                                    .addOption("snappy", "snappy")
                                    .addOption("deflate", "deflate")
                                    .addOption("bzip2", "bzip2")
                                    .addOption("xz", "xz"),
                                "compression"
                            )
                                .addField(
                                SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .showErrorsFor("partitionColumns"),
                                "partitionColumns"
                            )
                                .addField(TextBox("Record Name").bindPlaceholder(""), "recordName")
                                .addField(TextBox("Record Namespace").bindPlaceholder(""), "recordNamespace")
                                .addField(TextBox("Avro Schema").bindPlaceholder("json schema"), "avroschema")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        from gensim.models import Word2Vec
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        diagnostics = super(avro, self).validate(context, component)
        if len(component.properties.path) == 0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        from gensim.models import Word2Vec
        now_in_paris = pendulum.now('Europe/Paris')
        past = pendulum.now().subtract(minutes=2)
        print(now_in_paris)
        print(past.diff_for_humans())
        sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
        return newState

    class AvroFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: avro.AvroProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            import pendulum
            from gensim.models import Word2Vec
            now_in_paris = pendulum.now('Europe/Paris')
            past = pendulum.now().subtract(minutes=2)
            print(now_in_paris)
            print(past.diff_for_humans())
            sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
            model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
            reader = spark.read.format("avro")
            if self.props.ignoreExtension is not None:
                reader = reader.option("ignoreExtension", self.props.ignoreExtension)
            if self.props.modifiedBefore is not None:
                reader = reader.option("modifiedBefore", self.props.modifiedBefore)
            if self.props.modifiedAfter is not None:
                reader = reader.option("modifiedAfter", self.props.modifiedAfter)
            if self.props.recursiveFileLookup is not None:
                reader = reader.option("recursiveFileLookup", self.props.recursiveFileLookup)
            if self.props.pathGlobFilter is not None:
                reader = reader.option("pathGlobFilter", self.props.pathGlobFilter)
            if not isBlank(self.props.avroschema):
                reader = reader.option("avroSchema", self.props.avroschema)
            if self.props.schema is not None and self.props.useSchema:
                reader = reader.schema(self.props.schema)
            return reader.load(self.props.path)

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            import pendulum
            from gensim.models import Word2Vec
            now_in_paris = pendulum.now('Europe/Paris')
            past = pendulum.now().subtract(minutes=2)
            print(now_in_paris)
            print(past.diff_for_humans())
            sentences = [["cat", "say", "meow"], ["dog", "say", "woof"]]
            model = Word2Vec(sentences, vector_size=100, window=5, min_count=1)
            writer = in0.write.format("avro")
            if self.props.recordNamespace is not None:
                writer = writer.option("recordNamespace", self.props.recordNamespace)
            if self.props.compression is not None:
                writer = writer.option("compression", self.props.compression)
            if self.props.recordName is not None:
                writer = writer.option("recordName", self.props.recordName)
            if not isBlank(self.props.avroschema):
                writer = writer.option("avroSchema", self.props.avroschema)
            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)
            writer.save(self.props.path)
