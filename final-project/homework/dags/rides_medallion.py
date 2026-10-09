"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3 — SPEC.md, розділ 5.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.

Стан «що вже оброблено» живе в ДАНИХ, а не в розкладі: Bronze ідемпотентний за `_source_file`,
моделі dbt беруть лише рядки з `_ingested_at` вищим за межу, що вже є в них самих. Тому DAG
не використовує data_interval_start/end: запуск без нових файлів нічого не змінює, а ретрай
будь-якої задачі безпечний.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"  # dbt у окремому venv
# target/ і logs/ пишемо в /tmp контейнера, щоб не смітити у змонтованому проєкті.
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="rides_medallion",
    description="Bronze (Spark) -> Silver -> Gold (dbt) -> reconcile для подій ride-hailing",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rides", "medallion"],
) as dag:
    # Spark у local mode: landing -> bronze.raw_events (ідемпотентно за файлом).
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python bronze_job.py",
    )

    # Контракт Bronze: not_null, accepted_values для event_type (source:bronze у sources.yml).
    # cautious і тут: без нього eager-вибір підтягує assert_bronze_silver_reconcile (він теж
    # посилається на source bronze), а цьому тесту потрібен silver.events, якого ще немає.
    # Звірку між шарами робить пізніший крок reconcile.
    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=f"{DBT} test --select source:bronze --indirect-selection cautious {DBT_DIRS}",
    )

    # cautious: тест виконується лише тоді, коли в прогоні є ВСІ його parent-моделі.
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build --selector silver --indirect-selection cautious {DBT_DIRS}",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build --selector gold --indirect-selection cautious {DBT_DIRS}",
    )

    # Звірка між шарами: тести з тегом reconcile посилаються на моделі різних шарів.
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test --selector reconcile {DBT_DIRS}",
    )

    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
