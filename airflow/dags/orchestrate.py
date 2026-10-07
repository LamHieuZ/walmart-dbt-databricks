from airflow.sdk import dag, task


DBT_DIR = "/opt/airflow/walmart_dbt"
# Separate target/log dirs so Linux dbt never reads the Windows partial-parse cache
DBT = "/opt/dbt_venv/bin/dbt"
DBT_FLAGS = "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
@dag
def orchestrate():

    
    @task.bash
    def ingest():
        # Trigger the Databricks ingestion pipeline (Neon -> bronze) and wait for it
        return "/opt/dbt_venv/bin/python /opt/airflow/scripts/trigger_ingestion.py"

    @task.bash
    def source_freshness():
        # Manually check the freshness of the source data
        return f"cd {DBT_DIR} && {DBT} source freshness {DBT_FLAGS}"

    @task.bash
    def silver_technical():
        return f"cd {DBT_DIR} && {DBT} run --select silver {DBT_FLAGS}"
    @task.bash
    def silver_business():
        return f"cd {DBT_DIR} && {DBT} run --select silver_b {DBT_FLAGS}"

    @task.bash
    def silver_business_tests():
        return f"cd {DBT_DIR} && {DBT} test --select silver_b {DBT_FLAGS}"
    @task.bash
    def gold_ephemeral():
        return f"cd {DBT_DIR} && {DBT} run --select gold.ephemeral {DBT_FLAGS}"

    @task.bash
    def gold_dimensions():
        return f"cd {DBT_DIR} && {DBT} snapshot {DBT_FLAGS}"

    @task.bash
    def gold_facts():
        return f"cd {DBT_DIR} && {DBT} run --select gold.Fact {DBT_FLAGS}"


    ingest() >> source_freshness()>> silver_technical() >> silver_business() >> silver_business_tests() >> gold_ephemeral() >> gold_dimensions() >> gold_facts()
orchestrate_dag = orchestrate()