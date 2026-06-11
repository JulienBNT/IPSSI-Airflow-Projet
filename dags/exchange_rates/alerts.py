from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task, Variable

from exchange_rates.config import ALERT_THRESHOLD_DEFAULT

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "fx_postgres"

# Les deux dates les plus récentes présentes en table structurée
_SELECT_DATES_SQL = """
    SELECT DISTINCT rate_date
    FROM fx.exchange_rates
    ORDER BY rate_date DESC
    LIMIT 2;
"""

# Jointure base/quote sur les deux dates pour calculer l'écart relatif
_SELECT_DEVIATIONS_SQL = """
    SELECT
        c.base_currency,
        c.quote_currency,
        p.rate                                              AS previous_rate,
        c.rate                                              AS current_rate,
        ABS(c.rate - p.rate) / NULLIF(p.rate, 0) * 100    AS deviation_pct
    FROM fx.exchange_rates c
    JOIN fx.exchange_rates p
        ON  c.base_currency  = p.base_currency
        AND c.quote_currency = p.quote_currency
        AND p.rate_date      = %s
    WHERE c.rate_date = %s
      AND ABS(c.rate - p.rate) / NULLIF(p.rate, 0) * 100 > %s;
"""

# Idempotent : rejouer un run met à jour l'alerte sans doublon
_INSERT_ALERT_SQL = """
    INSERT INTO fx.alerts
        (base_currency, quote_currency, previous_rate, current_rate,
         deviation_pct, threshold_pct, rate_date_prev, rate_date_curr, run_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (base_currency, quote_currency, run_id) DO UPDATE
        SET current_rate  = EXCLUDED.current_rate,
            deviation_pct = EXCLUDED.deviation_pct,
            alerted_at    = now();
"""


@task(
    task_id="check_alerts",
    retries=2,
    retry_delay=timedelta(seconds=30),
    execution_timeout=timedelta(minutes=3),
)
def check_alerts(quality_result: dict, **context) -> dict:
    """Compare les taux courants avec l'exécution précédente et écrit dans fx.alerts.

    Idempotent : ON CONFLICT (base_currency, quote_currency, run_id) DO UPDATE.
    Le seuil (%) est lu depuis la Variable Airflow exchange_rate_alert_threshold.
    """
    threshold = float(
        Variable.get("exchange_rate_alert_threshold", default=str(ALERT_THRESHOLD_DEFAULT))
    )

    run_id = context["dag_run"].run_id
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    dates = hook.get_records(_SELECT_DATES_SQL)
    if len(dates) < 2:
        log.info(
            "[ALERTS] Historique insuffisant (%d date(s)) — aucune comparaison possible",
            len(dates),
        )
        return {"alerts_inserted": 0, "threshold_pct": threshold, "run_id": run_id}

    current_date, previous_date = dates[0][0], dates[1][0]

    deviations = hook.get_records(
        _SELECT_DEVIATIONS_SQL, parameters=(previous_date, current_date, threshold)
    )

    if deviations:
        rows = [
            (
                row[0],                    # base_currency
                row[1],                    # quote_currency
                row[2],                    # previous_rate
                row[3],                    # current_rate
                Decimal(str(row[4])),      # deviation_pct
                threshold,                 # threshold_pct
                previous_date,             # previous_date
                current_date,              # current_date
                run_id,                    # run_id
            )
            for row in deviations
        ]
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(_INSERT_ALERT_SQL, rows)
        conn.commit()

        for row in deviations:
            log.warning(
                "[ALERT] %s/%s — écart=%.4f%% > seuil=%.1f%% "
                "(précédent=%.6f @ %s, courant=%.6f @ %s)",
                row[0], row[1], float(row[4]), threshold,
                float(row[2]), previous_date, float(row[3]), current_date,
            )

    log.info(
        "[ALERTS] %d alerte(s) | seuil=%.1f%% | %s → %s | run=%s",
        len(deviations), threshold, previous_date, current_date, run_id,
    )
    return {
        "alerts_inserted": len(deviations),
        "threshold_pct": threshold,
        "rate_date_curr": str(current_date),
        "rate_date_prev": str(previous_date),
        "run_id": run_id,
    }
