from __future__ import annotations

import logging

import requests
from airflow.sdk import task, Variable

from exchange_rates.config import TIMEOUT_DEFAULT, API_URL_DEFAULT

log = logging.getLogger(__name__)


@task(task_id="extract_rates")
def extract_rates(**context) -> list:
    base: str = context["params"]["base"].strip().upper()
    quotes: list[str] = [c.strip().upper() for c in context["params"]["quotes"] if c.strip()]

    if len(quotes) < 5:
        raise ValueError(f"Au moins 5 devises requises, {len(quotes)} fournie(s) : {quotes}")

    timeout = int(Variable.get("exchange_rate_api_timeout", default=str(TIMEOUT_DEFAULT)))
    api_url = Variable.get("exchange_rate_api_url", default=API_URL_DEFAULT)
    url = f"{api_url}?base={base}&quotes={','.join(quotes)}"

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
