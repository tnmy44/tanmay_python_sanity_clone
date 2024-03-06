from staging_abhisheks_e2etests_python_sanity_project_staging_airflow253_python_composer.utils import *

def Email_1_1():
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1_1",
        to = "abhisheks@prophecy.io",
        subject = "Hello abhisheks",
        html_content = "Python sanity job run buddy",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "email_default",
    )
