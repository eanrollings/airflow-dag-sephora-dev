from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


table_creation = BigQueryExecuteQueryOperator(
    task_id="make a table in bigquery",
    sql="""
    SELECT 0 AS Test_1, 1 AS Test_2
    """,
    destination_dataset_table=f"digitas-sephora.Test_Data.airflow_example",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="sephora_bigquery_connection",
    use_legacy_sql=False,
)

table_creation