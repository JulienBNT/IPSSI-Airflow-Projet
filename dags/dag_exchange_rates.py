from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, Param

from exchange_rates.config import BASE_DEFAULT, CURRENCIES_DEFAULT
from exchange_rates.lifecycle import on_task_failure, log_start, log_anomaly, log_end
from exchange_rates.extract import extract_rates
from exchange_rates.load import load_raw
from exchange_rates.transform import transform_rates
from exchange_rates.quality import quality_check
from exchange_rates.alerts import check_alerts


@dag(
    dag_id="exchange_rates_pipeline",
    description="Récupération des taux de change via Frankfurter API",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": on_task_failure,
    },
    params={
        "base": Param(
            default=BASE_DEFAULT,
            type="string",
            title="Devise de base",
            description="Code ISO 4217 de la devise depuis laquelle les taux sont calculés.",
            minLength=3,
            maxLength=3,
        ),
        "quotes": Param(
            default=CURRENCIES_DEFAULT.split(","),
            type="array",
            title="Devises cibles",
            description="Liste des codes ISO 4217 des devises à récupérer (5 minimum).",
            items={"type": "string", "minLength": 3, "maxLength": 3},
            minItems=5,
        ),
    },
    tags=["exchange_rates", "api", "finance"],
)
def exchange_rates_pipeline():
    log_start_task = log_start()
    raw = extract_rates()
    raw_id = load_raw(raw)
    result = transform_rates(raw)
    quality = quality_check(raw, raw_id)
    alerts = check_alerts(quality)
    anomaly = log_anomaly()
    log_end_task = log_end()

    log_start_task >> raw >> raw_id
    raw_id >> [result, quality]
    quality >> alerts
    [raw, raw_id, result, quality, alerts] >> anomaly
    [result, quality, alerts, anomaly] >> log_end_task


exchange_rates_pipeline()
