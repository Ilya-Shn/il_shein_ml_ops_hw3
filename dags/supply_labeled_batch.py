from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.supply import sample, append, prune


@dag(schedule="*/5 * * * *", start_date=datetime(2025, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)})
def supply_labeled_batch():
    value = task(sample)()
    value = task(append)(value)
    value = task(prune)(value)


supply_labeled_batch()
