from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def Python_1():

    def return_method():
        return [{
                  "c1_data": [
{
"data" : join_2_strings(squared_numbers(2), "test value")}],
                  "c2_data": [
{
"data" : join_2_strings(squared_numbers(3), "test value") == "hello"}],
                  "c3_data": 1
                },
                {
                  "c1_data": [
{
"data" : join_2_strings(squared_numbers(4), "test value iteration2")}],
                  "c2_data": [
{
"data" : join_2_strings(squared_numbers(5), "test value") != "hello"}],
                  "c3_data": 2
                }]

    import json
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(task_id = "Python_1", python_callable = return_method, show_return_value_in_logs = True)
