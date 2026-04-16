"""
dags/finlake_pipeline.py

Apache Airflow DAG — FinLake Daily Batch Pipeline

Orchestrates the entire batch ingestion and transformation process:
Bronze (Ingest) -> Silver (Clean + SCD2 + DQ) -> Gold (Curated) -> Reconcile

Design principles:
  - RETRIES: Every task is idempotent and retries automatically on failure
    (transient network errors shouldn't wake you up at 3am).
  - DEPENDENCY GRAPH: The bitwise operators (>>) explicitly define what
    waits for what. Gold can't run if Silver DQ fails.
  - MODULARITY: We use BashOperator to call the exact same Python scripts
    we run locally via the Makefile.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments applied to every task in this DAG
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    # In production, start_date is set to when the pipeline was "turned on"
    "start_date": datetime.utcnow() - timedelta(days=1),
    "email_on_failure": False,
    "email_on_retry": False,
    # Production pipelines MUST have retries for transient failures
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="finlake_daily_batch_pipeline",
    default_args=default_args,
    description="End-to-End Medallion Architecture Pipeline",
    # Run daily at 18:00 UTC (after US markets close)
    schedule_interval="0 18 * * *",
    catchup=False,  # Don't backfill past runs automatically
    tags=["finlake", "daily", "medallion", "lakehouse"],
) as dag:

    # ── BRONZE STAGE ─────────────────────────────
    # Run both ingestions in parallel. They load raw data to MinIO.
    ingest_ohlcv = BashOperator(
        task_id="bronze_ingest_ohlcv",
        bash_command="cd /opt/airflow && python -m src.ingestion.batch_ingestor",
    )

    ingest_company = BashOperator(
        task_id="bronze_ingest_company_metadata",
        bash_command="cd /opt/airflow && python -m src.ingestion.company_ingestor",
    )

    # ── SILVER STAGE ─────────────────────────────
    # Transforms Bronze files into Iceberg tables.
    # Depends on Bronze finishing first.
    transform_silver_ohlcv = BashOperator(
        task_id="silver_transform_ohlcv",
        bash_command="cd /opt/airflow && python -m src.transformation.bronze_to_silver",
    )

    run_scd2 = BashOperator(
        task_id="silver_scd2_company_dim",
        bash_command="cd /opt/airflow && python -m src.transformation.scd2_company_dim",
    )

    # Data Quality Validation — blocks downstream if it fails
    run_dq_checks = BashOperator(
        task_id="silver_data_quality_validate",
        bash_command="cd /opt/airflow && python -m src.quality.dq_checks",
    )

    # ── GOLD STAGE ───────────────────────────────
    # Generates final analytics tables.
    # Depends on Silver DQ passing.
    transform_gold = BashOperator(
        task_id="gold_transform_analytics",
        bash_command="cd /opt/airflow && python -m src.transformation.silver_to_gold",
    )

    # ── RECONCILIATION ───────────────────────────
    # Final check: do row counts and checksums match across all 3 layers?
    reconcile = BashOperator(
        task_id="verify_reconciliation",
        bash_command="cd /opt/airflow && python -m src.quality.reconciliation",
    )


    # ── PIPELINE GRAPH (Dependencies) ────────────
    # This defines the visual flow in the Airflow UI
    #
    #   [ingest_ohlcv] ----> [silver_ohlcv] --┐
    #                                         v
    #                                    [dq_checks] -> [gold] -> [reconcile]
    #                                         ^
    #   [ingest_company] --> [silver_scd2] ---┘

    ingest_ohlcv >> transform_silver_ohlcv
    ingest_company >> run_scd2

    [transform_silver_ohlcv, run_scd2] >> run_dq_checks
    run_dq_checks >> transform_gold
    transform_gold >> reconcile
