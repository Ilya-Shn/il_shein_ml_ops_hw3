from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.retrain import prepare, fit, evaluate, publish


@dag(schedule="*/5 * * * *", start_date=datetime(2025, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=15)})
def retrain_catboost():
    value = task(prepare)()
    value = task(fit)(value)
    value = task(evaluate)(value)
    value = task(publish)(value)


retrain_catboost()
