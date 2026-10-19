from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.quality import check, report


@dag(schedule="*/5 * * * *", start_date=datetime(2025, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)})
def data_quality():
    value = task(check)()
    value = task(report)(value)


data_quality()
