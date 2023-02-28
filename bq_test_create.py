import datetime

# [START composer_notify_failure]
from airflow import models

# [END composer_notify_failure]
# [START composer_bash_bq]
#from airflow.operators import bash

# [END composer_bash_bq]
# [START composer_email]
#from airflow.operators import email

# [END composer_email]
# [START composer_bigquery]
from airflow.providers.google.cloud.operators import bigquery
#from airflow.providers.google.cloud.transfers import bigquery_to_gcs

# [END composer_bigquery]
#from airflow.utils import trigger_rule

project_id = 'digitas-sephora'
bq_dataset_name = "Test_Data"
#bq_recent_questions_table_id = "recent_questions"
bq_most_popular_table_id = "most_popular"
#gcs_bucket = "{{var.value.gcs_bucket}}"
#output_file = f"{gcs_bucket}/recent_questions.csv"
#location = "US"
#project_id = "{{var.value.gcp_project}}"

#max_query_date = "2023-02-02"
#min_query_date = "2023-02-01"
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

POPULAR_QUERY = f"""
        CREATE OR REPLACE TABLE `digitas-sephora.Test_Data.most_popular`
        SELECT 0 AS Test_1, 1 AS Test_2
        """

# [START composer_notify_failure]
default_dag_args = {
    "start_date": yesterday,
    # Email whenever an Operator in the DAG fails.
    # "email": "{{var.value.email}}",
    # "email_on_failure": True,
    # "email_on_retry": False,
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=1),
    "project_id": project_id,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'Table Creation Query',
        description="something or another",
        schedule_interval="@daily",
        start_date=datetime.datetime(2023, 1, 1),
        catchup=False,
        default_args=default_dag_args) as dag:

    bq_most_popular_query = bigquery.BigQueryInsertJobOperator(
        task_id="bq_most_popular_question_query",
        gcp_conn_id='sephora_bigquery_connection',
        configuration={
            "query": {
                "query": POPULAR_QUERY,
                "useLegacySql": False,
                #"destinationTable": {
                #    "projectId": project_id,
                #    "datasetId": bq_dataset_name,
                #    "tableId": bq_most_popular_table_id,
                #},
            }
        },
        #location=location,
    )

bq_most_popular_query