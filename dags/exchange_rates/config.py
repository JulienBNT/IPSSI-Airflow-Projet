from __future__ import annotations

import os

TIMEOUT_DEFAULT = int(os.environ.get("AIRFLOW_VAR_EXCHANGE_RATE_API_TIMEOUT", 10))
API_URL_DEFAULT = os.environ.get("AIRFLOW_VAR_EXCHANGE_RATE_API_URL", "https://api.frankfurter.dev/v2/rates")
BASE_DEFAULT = os.environ.get("AIRFLOW_VAR_EXCHANGE_RATE_BASE", "EUR")
CURRENCIES_DEFAULT = os.environ.get("AIRFLOW_VAR_EXCHANGE_RATE_CURRENCIES", "USD,GBP,JPY,CHF,CAD,AUD")

# seuil de fraîcheur (en jours) — un taux dont la date dépasse
FRESHNESS_DAYS_DEFAULT = int(os.environ.get("AIRFLOW_VAR_EXCHANGE_RATE_FRESHNESS_DAYS", 3))
