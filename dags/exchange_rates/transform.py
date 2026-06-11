from __future__ import annotations

import logging

from airflow.sdk import task

log = logging.getLogger(__name__)


@task(task_id="transform_rates", multiple_outputs=False)
def transform_rates(raw_data: list) -> dict:
    required_keys = {"date", "base", "quote", "rate"}
    for item in raw_data:
        missing = required_keys - item.keys()
        if missing:
            raise ValueError(f"Champs manquants dans un item : {missing} — {item}")

    base_currency = raw_data[0]["base"]
    fetch_date = raw_data[0]["date"]

    result = {
        "base": base_currency,
        "date": fetch_date,
        "rates": {item["quote"]: item["rate"] for item in raw_data},
    }

    log.info("Résultat — base=%s | date=%s | devises=%s", base_currency, fetch_date, result["rates"])
    return result
