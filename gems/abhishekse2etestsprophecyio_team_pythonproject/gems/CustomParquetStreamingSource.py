from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.WorkflowContext import WorkflowContext


class ParquetFormat(DatasetSpec):
    name: str = "CustomParquetStreamingSource"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/spark-streaming/streaming-sources-and-targets/streaming-file-apps"
    mode = "stream"
    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class ParquetProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        path: str = ""
        uri: Optional[str] = None
        checkpointLocation: str = ""
        loaderType: Optional[str] = None
        listingMode: Optional[str] = None
        mergeSchema: Optional[bool] = None
        datetimeRebaseMode: Optional[str] = None
        int96RebaseMode: Optional[str] = None
        compression: Optional[str] = None
        partitionColumns: Optional[List[str]] = None
        writeMode: Optional[str] = None
        pathGlobFilter: Optional[str] = None
        modifiedBefore: Optional[str] = None
        modifiedAfter: Optional[str] = None
        recursiveFileLookup: Optional[bool] = None
        maxFilesPerTrigger: Optional[str] = None
        latestFirst: Optional[bool] = None
        fileNameOnly: Optional[bool] = None
        maxFileAge: Optional[str] = None
        cleanSource: Optional[str] = None
        sourceArchiveDir: Optional[str] = None
        trigger: Optional[str] = None
        triggerInterval: Optional[str] = None
        queryName: Optional[str] = None
        allowOverwrites: Optional[bool] = None
        backfillInterval: Optional[str] = None
        fetchParallelism: Optional[str] = None
        includeExistingFiles: Optional[bool] = None
        maxBytesPerTrigger: Optional[str] = None
        pathRewrites: Optional[str] = None
        validateOptions: Optional[bool] = None
        useIncrementalListing: Optional[str] = None
        ignoreCorruptFiles: Optional[bool] = None
        ignoreMissingFiles: Optional[bool] = None
        inferColumnTypes: Optional[bool] = None
        schemaEvolutionMode: Optional[str] = None
        schemaLocation: Optional[str] = None
        numBytes: Optional[str] = None
        numFiles: Optional[str] = None
        rescuedDataColumn: Optional[str] = None
        readerCaseSensitive: Optional[bool] = None


    def sourceDialog(self) -> DatasetDialog:
        commonOptions = FieldPicker(height="100%") \
            .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup") \
            .addField(TextBox("Max Files Per Trigger").bindPlaceholder(""), "maxFilesPerTrigger") \
            .addField(TextBox("Max File Age").bindPlaceholder(""), "maxFileAge") \
            .addField(TextBox("Path Glob Filter"), "pathGlobfilter")

        autoLoaderOptions = commonOptions \
            .addField(Checkbox("Allow Overwrites"), "allowOverwrites") \
            .addField(TextBox("Backfill Interval").bindPlaceholder("1 day"), "backfillInterval") \
            .addField(Checkbox("Ignore Corrupt Files"), "ignoreCorruptFiles") \
            .addField(Checkbox("Include Existing Files"), "includeExistingFiles") \
            .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore") \
            .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter") \
            .addField(Checkbox("Validate Options"), "validateOptions") \
            .addField(TextBox("Rescued Data Column").bindPlaceholder(""), "rescuedDataColumn") \
            .addField(Checkbox("Reader Case Sensitive"), "readerCaseSensitive") \
            .addField(
                SelectBox("Use Incremental Listing")
                    .addOption("Auto", "auto")
                    .addOption("True", "true")
                    .addOption("False", "false"),
                "useIncrementalListing"
        )

        return DatasetDialog("parquet") \
            .addSection("LOCATION",
                        StackLayout(height="auto")
                        .addElement(
                            StackLayout(direction="horizontal", gap="2rem", height="auto")
                            .addElement(TitleElement("Loader Type:"))
                            .addElement(RadioGroup("", orientation="horizontal")
                                        .addOption("Standard", "standard")
                                        .addOption("AutoLoader", "autoloader")
                                        .bindProperty("loaderType"))
                        )
                        .addElement(
                            Condition()
                            .ifEqual(PropExpr("component.properties.loaderType"), StringExpr("standard"))
                            .then(
                                ColumnsLayout(gap="1rem", height="auto")
                                .addColumn(
                                    ScrollBox()
                                    .addElement(
                                        StackLayout(height="100%")
                                        .addElement(
                                            StackItem(grow=1).addElement(
                                                commonOptions
                                                .addField(Checkbox("Latest First"), "latestFirst")
                                                .addField(Checkbox("File Name Only"), "fileNameOnly")
                                                .addField(
                                                    SelectBox("Clean Source")
                                                    .addOption("Archive", "archive")
                                                    .addOption("Delete", "delete")
                                                    .addOption("Off", "off"),
                                                    "cleanSource"
                                                )
                                                .addField(TextBox("Source Archive Dir").bindPlaceholder(""), "sourceArchiveDir")
                                            )
                                        )
                                    ),
                                    "auto"
                                )
                                .addColumn(TargetLocation("path"))
                            )
                        )
                        .addElement(
                            Condition()
                            .ifEqual(PropExpr("component.properties.loaderType"), StringExpr("autoloader"))
                            .then(
                                StackLayout(height="auto")
                                .addElement(
                                    StackLayout(direction="horizontal", gap="2rem", height="auto")
                                    .addElement(TitleElement("Listing Mode:"))
                                    .addElement(
                                        RadioGroup("", defaultValue="directory", orientation="horizontal")
                                        .addOption("Directory Listing", "directory")
                                        .addOption("File Notification", "notification")
                                        .bindProperty("listingMode")
                                    )
                                )
                            )
                        )
                        .addElement(
                            Condition()
                            .ifEqual(PropExpr("component.properties.listingMode"), StringExpr("directory"))
                            .then(
                                ColumnsLayout(gap="1rem", height="auto")
                                .addColumn(
                                    ScrollBox()
                                    .addElement(
                                        StackLayout(height="auto")
                                        .addElement(
                                            StackItem(grow=1).addElement(
                                                autoLoaderOptions
                                            )
                                        )
                                    ),
                                    "auto"
                                )
                                .addColumn(TargetLocation("path"))
                            )
                        )
                        .addElement(
                            Condition()
                            .ifEqual(PropExpr("component.properties.listingMode"), StringExpr("notification"))
                            .then(
                                ColumnsLayout(gap="1rem", height="auto")
                                .addColumn(
                                    ScrollBox()
                                    .addElement(
                                        StackLayout(height="auto")
                                        .addElement(
                                            StackItem(grow=1).addElement(
                                                autoLoaderOptions
                                                .addField(TextBox("Fetch Parallelism").bindPlaceholder(""), "fetchParallelism")
                                                .addField(TextArea("Path Rewrites", 2, placeholder="Must be a valid JSON"), "pathRewrites")
                                            )
                                        )
                                    ),
                                    "auto"
                                )
                                .addColumn(
                                    StackLayout().addElement(
                                        StackLayout(direction="vertical", gap="1rem", height="100%")
                                        .addElement(TextBox("URI")
                                                    .bindPlaceholder("Enter URI of a AWS SQS queue")
                                                    .bindProperty("uri"))
                                        .addElement(TargetLocation("path")
                                        )
                                    ),
                                )
                            )
                        )
                        ) \
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
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                                .addField(Checkbox("Merge schema"), "mergeSchema")
                                .addField(
                                SelectBox("Datetime Rebase Mode")
                                    .addOption("EXCEPTION", "EXCEPTION")
                                    .addOption("CORRECTED", "CORRECTED")
                                    .addOption("LEGACY", "LEGACY"),
                                "datetimeRebaseMode"
                            )
                                .addField(
                                SelectBox("Int96 Rebase Mode")
                                    .addOption("EXCEPTION", "EXCEPTION")
                                    .addOption("CORRECTED", "CORRECTED")
                                    .addOption("LEGACY", "LEGACY"),
                                "int96RebaseMode"
                            )
                               .addField(
                                Condition()
                                    .ifEqual(PropExpr("component.properties.loaderType"), StringExpr("autoloader"))
                                    .then(
                                        SchemaColumnsDropdown("Partition Columns")
                                            .withMultipleSelection()
                                            .bindSchema("schema")
                                            .showErrorsFor("partitionColumns")
                                    ),
                                "partitionColumns"
                            )
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
        return DatasetDialog("parquet") \
            .addSection("LOCATION", TargetLocation("path")) \
            .addSection("CHECKPOINT", TargetLocation("checkpointLocation")) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%")).addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2, placeholder="Dataset description..."),
                                "description",
                                True
                            )
                                .addField(
                                SelectBox("Write Mode")
                                    .addOption("append", "append")
                                    .addOption("complete", "complete"),
                                "writeMode"
                            )
                                .addField(
                                SchemaColumnsDropdown("Partition Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .showErrorsFor("partitionColumns"),
                                "partitionColumns"
                            )
                                .addField(
                                SelectBox("Compression Codec")
                                    .addOption("none", "none")
                                    .addOption("uncompressed", "uncompressed")
                                    .addOption("gzip", "gzip")
                                    .addOption("lz4", "lz4")
                                    .addOption("snappy", "snappy")
                                    .addOption("lzo", "lzo")
                                    .addOption("brotli", "brotli")
                                    .addOption("zstd", "zstd"),
                                "compression"
                            )
                                .addField(
                                SelectBox("Trigger")
                                    .addOption("Default", "default")
                                    .addOption("Once", "once")
                                    .addOption("Now", "now")
                                    .addOption("Fixed Interval", "fixedInterval"),
                                "trigger",
                                True
                            )
                                .addField(
                                    Condition()
                                        .ifEqual(PropExpr("component.properties.trigger"), StringExpr("fixedInterval"))
                                        .then(TextBox("Trigger Interval").bindPlaceholder("").bindProperty("triggerInterval")),
                                "triggerInterval",
                                True
                            )
                        )
                    )
                ),
                "auto"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(ParquetFormat, self).validate(context, component)
        if component.properties.listingMode=="notification" and component.properties.loaderType =="autoloader":
            if len(component.properties.uri) == 0:
                diagnostics.append(
                    Diagnostic("properties.URI", "URI variable cannot be empty [Location]", SeverityLevelEnum.Error))
        elif len(component.properties.path)==0:
            diagnostics.append(
                Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevelEnum.Error))
        if component.properties.schema is None:
            diagnostics.append(Diagnostic(
                "properties.schema",
                "schema field cannot be empty [Properties]",
                SeverityLevelEnum.Error
            ))
        # TODO: Check only for target, which we don't know how as of now
        #if len(component.properties.checkpointLocation) == 0:
        #    diagnostics.append(
        #        Diagnostic("properties.checkpointLocation", "checkpointLocation cannot be empty", SeverityLevelEnum.Error))

        if component.properties.trigger == "fixedInterval" and component.properties.triggerInterval is None:
            diagnostics.append(
                Diagnostic("properties.triggerInterval",
                           "triggerInterval can't be empty if fixedInterval trigger is used",
                           SeverityLevelEnum.Error)
            )
        if component.properties.cleanSource == "archive" and len(component.properties.sourceArchiveDir) == 0:
            diagnostics.append(
                Diagnostic("properties.sourceArchiveDir",
                            "sourceArchiveDir can't be empty if cleanSource is set to archive mode",
                            SeverityLevelEnum.Error)
            )
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        newState = newState.bindProperties(replace(newState.properties, queryName=newState.metadata.label + "_" + newState.id))
        return newState

    class ParquetFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: ParquetFormat.ParquetProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.readStream
            if self.props.loaderType == "standard":
                reader = reader.format("parquet")
                if self.props.latestFirst is not None:
                    reader = reader.option("latestFirst", self.props.latestFirst)
                if self.props.fileNameOnly is not None:
                    reader = reader.option("fileNameOnly", self.props.fileNameOnly)
                if self.props.cleanSource is not None:
                    reader = reader.option("cleanSource", self.props.cleanSource)
                if self.props.sourceArchiveDir is not None:
                    reader = reader.option("sourceArchiveDir", self.props.sourceArchiveDir)

            else:  # Specific to autoloader mode
                reader = reader.format("cloudFiles").option("cloudFiles.format", "parquet")
                if self.props.listingMode == "notification":
                    reader = reader.option("cloudFiles.useNotifications", True).option("cloudFiles.queueUrl", self.props.uri)
                    if self.props.fetchParallelism is not None:
                        reader = reader.option("cloudFiles.fetchParallelism", self.props.fetchParallelism)
                    if self.props.pathRewrites is not None:
                        reader = reader.option("cloudFiles.pathRewrites", self.props.pathRewrites)

                # Specific to autoloader directory mode
                if self.props.listingMode == "directory" and self.props.useIncrementalListing is not None:
                    reader = reader.option("cloudFiles.useIncrementalListing", self.props.useIncrementalListing)

                # Specific to both autoloader modes
                if self.props.allowOverwrites is not None:
                    reader = reader.option("cloudFiles.allowOverwrites", self.props.allowOverwrites)
                if self.props.backfillInterval is not None:
                    reader = reader.option("cloudFiles.backfillInterval", self.props.backfillInterval)
                if self.props.includeExistingFiles is not None:
                    reader = reader.option("cloudFiles.includeExistingFiles", self.props.includeExistingFiles)
                if self.props.partitionColumns is not None:
                    reader = reader.option("cloudFiles.partitionColumns", self.props.partitionColumns)
                if self.props.validateOptions is not None:
                    reader = reader.option("cloudFiles.validateOptions", self.props.validateOptions)
                if self.props.pathGlobFilter is not None:
                    reader = reader.option("pathGlobFilter", self.props.pathGlobFilter)
                if self.props.rescuedDataColumn is not None:
                    reader = reader.option("rescuedDataColumn", self.props.rescuedDataColumn)
                if self.props.readerCaseSensitive is not None:
                    reader = reader.option("readerCaseSensitive", self.props.readerCaseSensitive)
                if self.props.modifiedBefore is not None:
                    reader = reader.option("modifiedBefore", self.props.modifiedBefore)
                if self.props.modifiedAfter is not None:
                    reader = reader.option("modifiedAfter", self.props.modifiedAfter)

            # Common options bw streaming and autoloader
            if self.props.recursiveFileLookup is not None:
                reader = reader.option("recursiveFileLookup", self.props.recursiveFileLookup)
            if self.props.ignoreCorruptFiles is not None:
                reader = reader.option("ignoreCorruptFiles", self.props.ignoreCorruptFiles)
            if self.props.ignoreMissingFiles is not None:
                reader = reader.option("ignoreMissingFiles", self.props.ignoreMissingFiles)

            # Common options but diff names
            if self.props.maxFilesPerTrigger is not None:
                reader = reader.option("cloudFiles.maxFilesPerTrigger" if self.props.loaderType == "autoloader" else "maxFilesPerTrigger", self.props.maxFilesPerTrigger)
            if self.props.maxBytesPerTrigger is not None:
                reader = reader.option("cloudFiles.maxBytesPerTrigger" if self.props.loaderType == "autoloader" else "maxBytesPerTrigger", self.props.maxBytesPerTrigger)
            if self.props.maxFileAge is not None:
                reader = reader.option("cloudFiles.maxFileAge" if self.props.loaderType == "autoloader" else "maxFileAge", self.props.maxFileAge)

            # Format specific options
            if self.props.mergeSchema is not None:
                reader = reader.option("mergeSchema", self.props.mergeSchema)
            if self.props.datetimeRebaseMode is not None:
                reader = reader.option("datetimeRebaseMode", self.props.datetimeRebaseMode)
            if self.props.int96RebaseMode is not None:
                reader = reader.option("int96RebaseMode", self.props.int96RebaseMode)

            # Remaining options
            if self.props.schema is not None:
                reader = reader.schema(self.props.schema)
            if self.props.listingMode is None or self.props.listingMode == "directory":
                reader = reader.load(self.props.path)
            else:
                if self.props.path is not None:
                    reader = reader.load(self.props.path)
                else:
                    reader = reader.load()
            return reader

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.writeStream.format("parquet").option("path", self.props.path)\
                .option("checkpointLocation", self.props.checkpointLocation)

            if self.props.queryName is not None:
                writer = writer.queryName(self.props.queryName)
            if self.props.compression is not None:
                writer = writer.option("compression", self.props.compression)
            if self.props.writeMode is not None:
                writer = writer.outputMode(self.props.writeMode)
            if self.props.partitionColumns is not None and len(self.props.partitionColumns) > 0:
                writer = writer.partitionBy(*self.props.partitionColumns)
            if self.props.trigger is not None:
                if self.props.trigger == "once":
                    writer = writer.trigger(once=True)
                elif self.props.trigger == "now":
                    writer = writer.trigger(availableNow=True)
                elif self.props.trigger == "fixed_interval":
                    writer = writer.trigger(processingTime=self.props.triggerInterval)

            writer.start()
