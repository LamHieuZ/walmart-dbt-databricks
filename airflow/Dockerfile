FROM apache/airflow:3.1.0-python3.12

# dbt goes in its own virtualenv so its dependencies don't clash with Airflow's
USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow:0 /opt/dbt_venv

USER airflow
RUN /opt/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.5 \
        dbt-databricks==1.12.6 \
        psycopg2-binary \
        python-dotenv
