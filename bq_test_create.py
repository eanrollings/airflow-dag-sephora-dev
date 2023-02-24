from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow import models

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "retries": 2,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'is this needed',
        description="?",
        schedule_interval="@daily",
        start_date=datetime.datetime(2023, 1, 1),
        catchup=False,
        default_args=default_dag_args) as dag:

    table_creation = BigQueryExecuteQueryOperator(
        task_id="make a table in bigquery",
        sql="""
        CREATE TABLE `digitas-sephora.Test_Data.airflow_example` AS SELECT 0 AS Test_1, 1 AS Test_2
        """,
        gcp_conn_id="sephora_bigquery_connection",
        use_legacy_sql=False,
)

table_creation