from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

table_creation = BigQueryExecuteQueryOperator(
    task_id="make a table in bigquery",
    sql="""
    CREATE TABLE `digitas-sephora.Test_Data.airflow_example` AS SELECT 0 AS Test_1, 1 AS Test_2
    """,
    gcp_conn_id="sephora_bigquery_connection",
    use_legacy_sql=False,
)