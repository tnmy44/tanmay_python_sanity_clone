from com.main1.pythondepmanagement_1.graph.all_type_tab_iter.TableIterator_2.config.Config import (
    SubgraphConfig as TableIterator_2_Config
)
from com.main1.pythondepmanagement_1.graph.all_type_tab_iter.Subgraph_4_1_1.config.Config import (
    SubgraphConfig as Subgraph_4_1_1_Config
)
from prophecy.config import ConfigBase


class C_struct(ConfigBase):
    def __init__(
            self,
            prophecy_spark=None,
            c_short: int=0,
            c_int: int=0,
            c_long: int=0,
            c_decimal: float=0.0,
            c_float: float=0.0,
            c_boolean: bool=True,
            c_double: float=0.0,
            c_string: str="",
            c_date: str="",
            c_timestamp: str="",
            c_array_int: list=[],
            **kwargs
    ):
        self.c_short = c_short
        self.c_int = c_int
        self.c_long = c_long
        self.c_decimal = c_decimal
        self.c_float = c_float
        self.c_boolean = c_boolean
        self.c_double = c_double
        self.c_string = c_string
        self.c_date = c_date
        self.c_timestamp = c_timestamp
        self.c_array_int = c_array_int
        pass


class C_sg1_record(ConfigBase):
    def __init__(
            self,
            prophecy_spark=None,
            c_sg1_record_c_string: str="hello sir $$ yes this is me $$$$",
            c_sg1_record_c_boolean: bool=True,
            c_sg1_record_c_float: float=12.12,
            c_sg1_record_c_int: int=12,
            **kwargs
    ):
        self.c_sg1_record_c_string = c_sg1_record_c_string
        self.c_sg1_record_c_boolean = c_sg1_record_c_boolean
        self.c_sg1_record_c_float = c_sg1_record_c_float
        self.c_sg1_record_c_int = c_sg1_record_c_int
        pass


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            c_short: int=0,
            c_int: int=0,
            c_long: int=0,
            c_float: float=0.0,
            c_boolean: bool=True,
            c_double: float=0.0,
            c_date: str="",
            c_timestamp: str="",
            c_array_int: list=[],
            c_array_string: list=[],
            c_array_long: list=[],
            c_array_boolean: list=[],
            c_array_date: list=[],
            c_array_timestamp: list=[],
            c_array_float: list=[],
            c_array_decimal: list=[],
            c_struct: dict={},
            Subgraph_4_1_1: dict={},
            JDBC_URL: str="jdbc:mysql://3.101.152.38:3306/test_database",
            JDBC_SOURCE_TABLE: str="test_table",
            CONFIG_STR: str="jdbc_url-${JDBC_URL}",
            CONFIG_BOOLEAN: bool=True,
            CONFIG_DOUBLE: float=1.00123211232E7,
            CONFIG_INT: int=97987,
            CONFIG_FLOAT: float=4567546.5,
            CONFIG_SHORT: int=120,
            EXPR_COMPLEX_DATES: str="(((((date_add(date_trunc(date_format(date_sub(date_add(current_date(), 2), 2), 'yyyy MMM dd'), 'YEAR'), 1) < date_sub(current_timestamp(), 2)) OR (date_add(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'), 2) < date_add(from_utc_timestamp(c_timestamp, 'Asia/Seoul'), 2))) OR (next_day('2015-01-14', 'TU') < current_date())) OR ((to_date('2009-07-30 04:17:52') < to_timestamp(c_date)) OR (add_months(c_timestamp, 1) < add_months(c_date, 2)))) AND ((c_int % 2) = 0))",
            c_int_11: int=10,
            c_st_expr: str="concat(`c   short  --`, `c-int-column type`)",
            c_decimal_renamed: str="`c-decimal renamed`",
            c_repartition_expr: str="concat(`c  float`, `c--boolean`)",
            c_repartition_colname: str="`c_float-__  `",
            c_sql_pattern: str="%[^aeiou]@%",
            c_row_distributor_expr: str="(((col(\"`c_struct -- _  `.`c_double - of a struct _`\") > lit(20)) | (col(\"`c_date-for today`\") == lit(\"2005-04-16\"))) & col(\"`c_array-string  _ string`\")[1].like(\"%7%\"))",
            c_join_expr: str="(in0.`- c long` = in1.`- c long`)",
            c_1: int=1,
            c_0: int=0,
            c_row_distributor: str="(((`c_struct -- _  `.`c_double - of a struct _` > 20) OR (`c_date-for today` = '2005-04-16')) AND `c_array-string  _ string`[1] LIKE '%7%')",
            c_order_by_expr: str="concat(c_int, c_long)",
            c_expr_deduplicate: str="concat(`c  float`, `c   short  --`)",
            c_rowdistributor_complex_expr: str="((p_string LIKE '%a%' OR RLIKE(p_string, '%A%')) OR ((`c_decimal  -  ` = 12321) AND `c_array-string  _ string`[0] LIKE '%3%'))",
            c_aggregate_expr: str="first(`c   short  --`)",
            c_aggregate_string: str="`c___-- string`",
            c_aggregate_float_name: str="`c float`",
            c_row_number: str="row_number()",
            c_long_wf: str="`- c long`",
            c_wf_orderby_expr: str="concat(`c -  boolean _  `, c_double)",
            c_regex1: str="^[_A-Za-z0-9-]+(\\\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\\\.[A-Za-z0-9]+)*(\\\\.[A-Za-z]{2,})",
            c_regex2: str="((?=.*)(?=.*[a-z])(?=.*[A-Z])(?=.*[@#%]).{6,20})",
            c_str: str="stringwith$$one#%^&*()-=!@#",
            c_config_1: str="this is a test!@#^&*()_=-",
            c_config_2: str="this is a test!@#^&*()_=-",
            c_config_3: str="this is a test!@#^&*()_=-",
            c_config_4: str="this is a test!@#^&*()_=-",
            c_config_5: str="this is a test!@#^&*()_=-",
            c_config_6: str="this is a test!@#^&*()_=-",
            c_config_7: str="this is a test!@#^&*()_=-",
            c_config_8: str=None,
            c_config_9: str=None,
            c_config_10: str=None,
            c_config_11: str="this is a test!@#^&*()_=- asd",
            c_config_12: str="this is a test!@#^&*()_=- asd",
            c_config_13: str="this is a test!@#^&*()_=- asd",
            c_config_14: str="this is a test!@#^&*()_=- asd",
            c_config_15: str="this is a test!@#^&*()_=- asd",
            c_config_16: str="this is a test!@#^&*()_=- asd",
            c_config_17: str="this is a test!@#^&*()_=- asd",
            c_config_18: str="this is a test!@#^&*()_=- asd",
            c_config_19: str="this is a test!@#^&*()_=- asd",
            c_config_20: str="this is a test!@#^&*()_=- asd",
            c_config_21: str="this is a test!@#^&*()_=- asd",
            c_config_22: str="this is a test!@#^&*()_=- asd",
            c_config_23: str="this is a test!@#^&*()_=- asd",
            c_config_24: str="this is a test!@#^&*()_=- asd",
            c_config_25: str="this is a test!@#^&*()_=- asd",
            c_config_26: str=None,
            c_config_27: str="this is a test!@#^&*()_=- asd",
            c_config_28: str="this is a test!@#^&*()_=- asd",
            c_config_29: str="this is a test!@#^&*()_=- asd",
            c_config_30: str="this is a test!@#^&*()_=- asd",
            c_config_31: str="this is a test!@#^&*()_=- asd",
            c_config_32: str="this is a test!@#^&*()_=- asd",
            c_config_33: str="this is a test!@#^&*()_=- asd",
            c_config_34: str="this is a test!@#^&*()_=- asd",
            c_config_35: str="this is a test!@#^&*()_=- asd",
            c_config_36: str="this is a test!@#^&*()_=- asdasd",
            c_config_37: str="this is a test!@#^&*()_=- asdasd",
            c_config_38: str=None,
            c_config_39: str="this is a test!@#^&*()_=- asdasd",
            c_config_40: str="this is a test!@#^&*()_=- asdasd",
            c_config_41: str="this is a test!@#^&*()_=- asdasd",
            c_config_42: str="this is a test!@#^&*()_=- asdasd",
            c_config_43: str="this is a test!@#^&*()_=- asdasd",
            c_config_44: str="this is a test!@#^&*()_=- asdasd",
            c_config_45: str="this is a test!@#^&*()_=- asdasd",
            c_config_46: str="this is a test!@#^&*()_=- asdasd",
            c_config_47: str="this is a test!@#^&*()_=- asdasd",
            c_config_48: str="this is a test!@#^&*()_=- asdasd",
            c_config_49: str="this is a test!@#^&*()_=- asdasd",
            c_config_50: str="this is a test!@#^&*()_=- asdasd",
            AI_MIN_DATETIME: str="2020-01-02 11:11:11",
            raw_schema: str="c1",
            pipeline_config: str="pipeline_value",
            path_var: str="dbfs:/Prophecy/qa_data/csv/CustomersDatasetInputWithHeader.csv",
            SNOW_USERNAME: str="cicdaccount",
            SNOW_PASSWORD: str="CuIqZ!9I32t@",
            c_string: str="test",
            c_sg1_spark_expressions: str="concat('a', 'b')",
            c_sg1_array_string: list=["this is first $$", "this is second $$"],
            c_sg1_record: dict={},
            TableIterator_2: dict={},
            **kwargs
    ):
        self.c_short = c_short
        self.c_int = c_int
        self.c_long = c_long
        self.c_float = c_float
        self.c_boolean = c_boolean
        self.c_double = c_double
        self.c_date = c_date
        self.c_timestamp = c_timestamp
        self.c_array_int = c_array_int
        self.c_array_string = c_array_string
        self.c_array_long = c_array_long
        self.c_array_boolean = c_array_boolean
        self.c_array_date = c_array_date
        self.c_array_timestamp = c_array_timestamp
        self.c_array_float = c_array_float
        self.c_array_decimal = c_array_decimal
        self.c_struct = self.get_config_object(
            prophecy_spark, 
            C_struct(prophecy_spark = prophecy_spark), 
            c_struct, 
            C_struct
        )
        self.Subgraph_4_1_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_4_1_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_4_1_1, 
            Subgraph_4_1_1_Config
        )
        self.JDBC_URL = JDBC_URL
        self.JDBC_SOURCE_TABLE = JDBC_SOURCE_TABLE
        self.CONFIG_STR = CONFIG_STR
        self.CONFIG_BOOLEAN = CONFIG_BOOLEAN
        self.CONFIG_DOUBLE = CONFIG_DOUBLE
        self.CONFIG_INT = CONFIG_INT
        self.CONFIG_FLOAT = CONFIG_FLOAT
        self.CONFIG_SHORT = CONFIG_SHORT
        self.EXPR_COMPLEX_DATES = EXPR_COMPLEX_DATES
        self.c_int_11 = c_int_11
        self.c_st_expr = c_st_expr
        self.c_decimal_renamed = c_decimal_renamed
        self.c_repartition_expr = c_repartition_expr
        self.c_repartition_colname = c_repartition_colname
        self.c_sql_pattern = c_sql_pattern
        self.c_row_distributor_expr = c_row_distributor_expr
        self.c_join_expr = c_join_expr
        self.c_1 = c_1
        self.c_0 = c_0
        self.c_row_distributor = c_row_distributor
        self.c_order_by_expr = c_order_by_expr
        self.c_expr_deduplicate = c_expr_deduplicate
        self.c_rowdistributor_complex_expr = c_rowdistributor_complex_expr
        self.c_aggregate_expr = c_aggregate_expr
        self.c_aggregate_string = c_aggregate_string
        self.c_aggregate_float_name = c_aggregate_float_name
        self.c_row_number = c_row_number
        self.c_long_wf = c_long_wf
        self.c_wf_orderby_expr = c_wf_orderby_expr
        self.c_regex1 = c_regex1
        self.c_regex2 = c_regex2
        self.c_str = c_str
        self.c_config_1 = c_config_1
        self.c_config_2 = c_config_2
        self.c_config_3 = c_config_3
        self.c_config_4 = c_config_4
        self.c_config_5 = c_config_5
        self.c_config_6 = c_config_6
        self.c_config_7 = c_config_7
        self.c_config_8 = c_config_8
        self.c_config_9 = c_config_9
        self.c_config_10 = c_config_10
        self.c_config_11 = c_config_11
        self.c_config_12 = c_config_12
        self.c_config_13 = c_config_13
        self.c_config_14 = c_config_14
        self.c_config_15 = c_config_15
        self.c_config_16 = c_config_16
        self.c_config_17 = c_config_17
        self.c_config_18 = c_config_18
        self.c_config_19 = c_config_19
        self.c_config_20 = c_config_20
        self.c_config_21 = c_config_21
        self.c_config_22 = c_config_22
        self.c_config_23 = c_config_23
        self.c_config_24 = c_config_24
        self.c_config_25 = c_config_25
        self.c_config_26 = c_config_26
        self.c_config_27 = c_config_27
        self.c_config_28 = c_config_28
        self.c_config_29 = c_config_29
        self.c_config_30 = c_config_30
        self.c_config_31 = c_config_31
        self.c_config_32 = c_config_32
        self.c_config_33 = c_config_33
        self.c_config_34 = c_config_34
        self.c_config_35 = c_config_35
        self.c_config_36 = c_config_36
        self.c_config_37 = c_config_37
        self.c_config_38 = c_config_38
        self.c_config_39 = c_config_39
        self.c_config_40 = c_config_40
        self.c_config_41 = c_config_41
        self.c_config_42 = c_config_42
        self.c_config_43 = c_config_43
        self.c_config_44 = c_config_44
        self.c_config_45 = c_config_45
        self.c_config_46 = c_config_46
        self.c_config_47 = c_config_47
        self.c_config_48 = c_config_48
        self.c_config_49 = c_config_49
        self.c_config_50 = c_config_50
        self.AI_MIN_DATETIME = AI_MIN_DATETIME
        self.raw_schema = raw_schema
        self.pipeline_config = pipeline_config
        self.path_var = path_var
        self.SNOW_USERNAME = SNOW_USERNAME
        self.SNOW_PASSWORD = SNOW_PASSWORD
        self.c_string = c_string
        self.c_sg1_spark_expressions = c_sg1_spark_expressions
        self.c_sg1_array_string = c_sg1_array_string
        self.c_sg1_record = self.get_config_object(
            prophecy_spark, 
            C_sg1_record(prophecy_spark = prophecy_spark), 
            c_sg1_record, 
            C_sg1_record
        )
        self.TableIterator_2 = self.get_config_object(
            prophecy_spark, 
            TableIterator_2_Config(prophecy_spark = prophecy_spark), 
            TableIterator_2, 
            TableIterator_2_Config
        )
        pass

    def update(self, updated_config):
        self.c_short = updated_config.c_short
        self.c_int = updated_config.c_int
        self.c_long = updated_config.c_long
        self.c_float = updated_config.c_float
        self.c_boolean = updated_config.c_boolean
        self.c_double = updated_config.c_double
        self.c_date = updated_config.c_date
        self.c_timestamp = updated_config.c_timestamp
        self.c_array_int = updated_config.c_array_int
        self.c_array_string = updated_config.c_array_string
        self.c_array_long = updated_config.c_array_long
        self.c_array_boolean = updated_config.c_array_boolean
        self.c_array_date = updated_config.c_array_date
        self.c_array_timestamp = updated_config.c_array_timestamp
        self.c_array_float = updated_config.c_array_float
        self.c_array_decimal = updated_config.c_array_decimal
        self.c_struct = updated_config.c_struct
        self.Subgraph_4_1_1 = updated_config.Subgraph_4_1_1
        self.JDBC_URL = updated_config.JDBC_URL
        self.JDBC_SOURCE_TABLE = updated_config.JDBC_SOURCE_TABLE
        self.CONFIG_STR = updated_config.CONFIG_STR
        self.CONFIG_BOOLEAN = updated_config.CONFIG_BOOLEAN
        self.CONFIG_DOUBLE = updated_config.CONFIG_DOUBLE
        self.CONFIG_INT = updated_config.CONFIG_INT
        self.CONFIG_FLOAT = updated_config.CONFIG_FLOAT
        self.CONFIG_SHORT = updated_config.CONFIG_SHORT
        self.EXPR_COMPLEX_DATES = updated_config.EXPR_COMPLEX_DATES
        self.c_int_11 = updated_config.c_int_11
        self.c_st_expr = updated_config.c_st_expr
        self.c_decimal_renamed = updated_config.c_decimal_renamed
        self.c_repartition_expr = updated_config.c_repartition_expr
        self.c_repartition_colname = updated_config.c_repartition_colname
        self.c_sql_pattern = updated_config.c_sql_pattern
        self.c_row_distributor_expr = updated_config.c_row_distributor_expr
        self.c_join_expr = updated_config.c_join_expr
        self.c_1 = updated_config.c_1
        self.c_0 = updated_config.c_0
        self.c_row_distributor = updated_config.c_row_distributor
        self.c_order_by_expr = updated_config.c_order_by_expr
        self.c_expr_deduplicate = updated_config.c_expr_deduplicate
        self.c_rowdistributor_complex_expr = updated_config.c_rowdistributor_complex_expr
        self.c_aggregate_expr = updated_config.c_aggregate_expr
        self.c_aggregate_string = updated_config.c_aggregate_string
        self.c_aggregate_float_name = updated_config.c_aggregate_float_name
        self.c_row_number = updated_config.c_row_number
        self.c_long_wf = updated_config.c_long_wf
        self.c_wf_orderby_expr = updated_config.c_wf_orderby_expr
        self.c_regex1 = updated_config.c_regex1
        self.c_regex2 = updated_config.c_regex2
        self.c_str = updated_config.c_str
        self.c_config_1 = updated_config.c_config_1
        self.c_config_2 = updated_config.c_config_2
        self.c_config_3 = updated_config.c_config_3
        self.c_config_4 = updated_config.c_config_4
        self.c_config_5 = updated_config.c_config_5
        self.c_config_6 = updated_config.c_config_6
        self.c_config_7 = updated_config.c_config_7
        self.c_config_8 = updated_config.c_config_8
        self.c_config_9 = updated_config.c_config_9
        self.c_config_10 = updated_config.c_config_10
        self.c_config_11 = updated_config.c_config_11
        self.c_config_12 = updated_config.c_config_12
        self.c_config_13 = updated_config.c_config_13
        self.c_config_14 = updated_config.c_config_14
        self.c_config_15 = updated_config.c_config_15
        self.c_config_16 = updated_config.c_config_16
        self.c_config_17 = updated_config.c_config_17
        self.c_config_18 = updated_config.c_config_18
        self.c_config_19 = updated_config.c_config_19
        self.c_config_20 = updated_config.c_config_20
        self.c_config_21 = updated_config.c_config_21
        self.c_config_22 = updated_config.c_config_22
        self.c_config_23 = updated_config.c_config_23
        self.c_config_24 = updated_config.c_config_24
        self.c_config_25 = updated_config.c_config_25
        self.c_config_26 = updated_config.c_config_26
        self.c_config_27 = updated_config.c_config_27
        self.c_config_28 = updated_config.c_config_28
        self.c_config_29 = updated_config.c_config_29
        self.c_config_30 = updated_config.c_config_30
        self.c_config_31 = updated_config.c_config_31
        self.c_config_32 = updated_config.c_config_32
        self.c_config_33 = updated_config.c_config_33
        self.c_config_34 = updated_config.c_config_34
        self.c_config_35 = updated_config.c_config_35
        self.c_config_36 = updated_config.c_config_36
        self.c_config_37 = updated_config.c_config_37
        self.c_config_38 = updated_config.c_config_38
        self.c_config_39 = updated_config.c_config_39
        self.c_config_40 = updated_config.c_config_40
        self.c_config_41 = updated_config.c_config_41
        self.c_config_42 = updated_config.c_config_42
        self.c_config_43 = updated_config.c_config_43
        self.c_config_44 = updated_config.c_config_44
        self.c_config_45 = updated_config.c_config_45
        self.c_config_46 = updated_config.c_config_46
        self.c_config_47 = updated_config.c_config_47
        self.c_config_48 = updated_config.c_config_48
        self.c_config_49 = updated_config.c_config_49
        self.c_config_50 = updated_config.c_config_50
        self.AI_MIN_DATETIME = updated_config.AI_MIN_DATETIME
        self.raw_schema = updated_config.raw_schema
        self.pipeline_config = updated_config.pipeline_config
        self.path_var = updated_config.path_var
        self.SNOW_USERNAME = updated_config.SNOW_USERNAME
        self.SNOW_PASSWORD = updated_config.SNOW_PASSWORD
        self.c_string = updated_config.c_string
        self.c_sg1_spark_expressions = updated_config.c_sg1_spark_expressions
        self.c_sg1_array_string = updated_config.c_sg1_array_string
        self.c_sg1_record = updated_config.c_sg1_record
        self.TableIterator_2 = updated_config.TableIterator_2
        pass

Config = SubgraphConfig()
