from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def generate_dataframe_log(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from typing import Optional, List, Dict
    from dataclasses import dataclass, field
    from abc import ABC
    
    from pyspark.sql.column import Column
    from pyspark.sql.functions import col
    from dataclasses import dataclass
    from typing import Optional, List, Dict
    from pyspark.sql.column import Column as sparkColumn


    @dataclass(frozen = True)
    class SColumn:
        expression: Optional[Column] = None

        @staticmethod
        def getSColumn(column: str):
            return SColumn(col(column))

        def column(self) -> sparkColumn:
            return self.expression

        def columnName(self) -> str:
            return self.expression._jc.toString()


    @dataclass(frozen = True)
    class SColumnExpression:
        target: str
        expression: SColumn
        description: str
        _row_id: Optional[str] = None

        @staticmethod
        def remove_backticks(s):
            if s.startswith("`") and s.endswith("`"):
                return s[1:- 1]
            else:
                return s

        @staticmethod
        def getColumnExpression(column: str):
            return SColumnExpression(column, SColumn.getSColumn(col(column)), "")

        @staticmethod
        def getColumnsFromColumnExpressionList(columnExpressions: list):
            columnList = []

            for expression in columnExpressions:
                columnList.append(expression.expression)

            return columnList

        def column(self) -> Column:

            if (self.expression.columnName() == SColumnExpression.remove_backticks(self.target)):
                return self.expression.expression

            return self.expression.expression.alias(self.target)


    @dataclass(frozen = True)
    class PerRowEvents():
        perRowEventType: SColumn
        perRowEventText: SColumn


    @dataclass(frozen = True)
    class LogProperties():
        componentName: str = ""
        subComponentName: str = ""
        perRowEventColumns: List[PerRowEvents] = field(default_factory = list)
        finalLogEventType: Optional[SColumn] = None
        finalLogEventText: Optional[SColumn] = None
        finalEventExtraColumns: List[SColumnExpression] = field(default_factory = list)
        loggerType: str = "General"

    props = LogProperties(  #skiptraversal
        componentName = "we", 
        subComponentName = "", 
        perRowEventColumns = [PerRowEvents(
           perRowEventType = SColumn(lit("start")), 
           perRowEventText = SColumn(concat(lit("starting logging for "), col("_c0")))
         )], 
        finalLogEventType = SColumn(col("_c0").cast(IntegerType())), 
        finalLogEventText = SColumn(col("_c0").cast(IntegerType())), 
        finalEventExtraColumns = [], 
        loggerType = "General"
    )
    inDFs = [in0]
    from prophecy.utils.transpiler.dataframe_fcns import generateLogOutput
    props = props
    dfToLog = inDFs[- 1] if props.loggerType == "Rollup" else inDFs[0]
    inputCount = 0

    if props.loggerType == "Join":
        for idx in range(0, len(inDFs)):
            inputCount = inputCount + inDFs[idx].count()
    elif props.loggerType == "AssignKeys":
        inputCount = inDFs[0].count() + inDFs[1].count()
    elif props.loggerType == "Rollup":
        inputCount = inDFs[- 1].count()
    else:
        inputCount = inDFs[0].count()

    outputCount = 0

    if props.loggerType == "AssignKeys":
        for idx in range(2, len(inDFs)):
            outputCount = outputCount + inDFs[idx].count()
    else:
        outputCount = 0

    finalEventExtraColumnsMap = dict([(s.target, s.expression.column()) for s in props.finalEventExtraColumns])

    if len(props.perRowEventColumns) == 0:
        perRowEventTypes = None
        perRowEventTexts = None
    else:
        perRowEventTypes = array(*[c.perRowEventType.column() for c in props.perRowEventColumns])
        perRowEventTexts = array(*[c.perRowEventText.column() for c in props.perRowEventColumns])

    fLogEventType = None if props.finalLogEventType is None else props.finalLogEventType.column()
    fLogEventText = None if props.finalLogEventText is None else props.finalLogEventText.column()

    return generateLogOutput(
        dfToLog,
        spark,
        props.componentName,
        props.subComponentName,
        perRowEventTypes,
        perRowEventTexts,
        inputCount,
        outputCount,
        fLogEventType,
        fLogEventText,
        finalEventExtraColumnsMap
    )
