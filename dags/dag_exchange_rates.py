from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task, Param, Variable

log = logging.getLogger(__name__)

TIMEOUT_DEFAULT = 10


def _on_task_failure(context: dict) -> None:
    ti = context["task_instance"]
    exception = context.get("exception")
    log.error(
        "[FAIL] DAG=%s | task=%s | run=%s | tentative=%s | erreur=%s: %s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        type(exception).__name__,
        exception,
    )


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
        "on_failure_callback": _on_task_failure,
    },
    params={
        "base": Param(
            default="EUR",
            type="string",
            title="Devise de base",
            description="Code ISO 4217 de la devise depuis laquelle les taux sont calculés.",
            minLength=3,
            maxLength=3,
        ),
        "quotes": Param(
            default=["USD", "GBP", "JPY", "CHF", "CAD", "AUD"],
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

    @task(task_id="log_start")
    def log_start() -> None:
        log.info("[START] Pipeline exchange_rates_pipeline démarré")

    @task(task_id="extract_rates")
    def extract_rates(**context) -> list:
        params = context["params"]
        base: str = params["base"].strip().upper()
        quotes: list[str] = [q.strip().upper() for q in params["quotes"] if q.strip()]

        if len(quotes) < 5:
            raise ValueError(
                f"Au moins 5 devises requises, {len(quotes)} fournie(s) : {quotes}"
            )

        timeout = int(Variable.get("exchange_rate_api_timeout", default=str(TIMEOUT_DEFAULT)))
        url = f"https://api.frankfurter.dev/v2/rates?base={base}&quotes={','.join(quotes)}"

        log.info("Appel API Frankfurter — URL=%s", url)

        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            log.error("Timeout après %ss lors de l'appel à %s", timeout, url)
            raise
        except requests.exceptions.HTTPError as exc:
            log.error("Erreur HTTP %s : %s", exc.response.status_code, exc)
            raise
        except requests.exceptions.RequestException as exc:
            log.error("Erreur réseau inattendue : %s", exc)
            raise

        raw_data = response.json()

        if not isinstance(raw_data, list) or len(raw_data) == 0:
            raise ValueError(f"Réponse inattendue de l'API : {type(raw_data).__name__} — {raw_data}")

        log.info("Réponse reçue — %d paires récupérées (date=%s)", len(raw_data), raw_data[0].get("date"))
        return raw_data

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

    @task(task_id="log_anomaly", trigger_rule="one_failed")
    def log_anomaly(**context) -> None:
        ti = context["task_instance"]
        log.error(
            "[ANOMALY] DAG=%s | run=%s | une tâche upstream a échoué — pipeline interrompu",
            ti.dag_id,
            ti.run_id,
        )

    @task(task_id="log_end", trigger_rule="all_done")
    def log_end() -> None:
        log.info("[END] Pipeline exchange_rates_pipeline terminé")

    log_start_task = log_start()
    raw = extract_rates()
    result = transform_rates(raw)
    anomaly = log_anomaly()
    log_end_task = log_end()

    log_start_task >> raw
    [raw, result] >> anomaly
    [result, anomaly] >> log_end_task


exchange_rates_pipeline()
