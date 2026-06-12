from __future__ import annotations

import json
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task, Variable

from exchange_rates.config import API_URL_DEFAULT

log = logging.getLogger(__name__)

# Connection ID Airflow vers PostgreSQL (Admin > Connections)
POSTGRES_CONN_ID = "fx_postgres"

# ON CONFLICT (run_id, base_currency) => ingestion idempotente : rejouer un run
# ne crée pas de doublon, on rafraîchit la ligne et on récupère son raw_id.
INSERT_RAW_SQL = """
    INSERT INTO fx.raw_exchange_rates
        (payload, base_currency, requested_quotes, source_endpoint,
         http_status, dag_id, run_id, logical_date)
    VALUES (%s::jsonb, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, base_currency) DO UPDATE
        SET payload          = EXCLUDED.payload,
            requested_quotes = EXCLUDED.requested_quotes,
            source_endpoint  = EXCLUDED.source_endpoint,
            ingested_at      = now()
    RETURNING raw_id;
"""


@task(task_id="load_raw")
def load_raw(raw_data: list, **context) -> int:
    """Insère la réponse brute de l'API dans fx.raw_exchange_rates (horodatée).

    Récupère la liste renvoyée par extract_rates() et la stocke telle quelle
    (JSONB) via la Connection ID PostgreSQL. Retourne le raw_id, qui sert de
    lien de traçabilité (lineage) pour les tâches en aval.
    """
    if not raw_data:
        raise ValueError("Aucune donnée brute à ingérer")

    base_currency = raw_data[0].get("base")
    quotes = sorted({item.get("quote") for item in raw_data if item.get("quote")})
    api_url = Variable.get("exchange_rate_api_url", default=API_URL_DEFAULT)
    source_endpoint = f"{api_url}?base={base_currency}&quotes={','.join(quotes)}"

    dag_run = context["dag_run"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    raw_id = hook.get_first(
        INSERT_RAW_SQL,
        parameters=(
            json.dumps(raw_data),          # payload brut conservé intégralement
            base_currency,
            ",".join(quotes),
            source_endpoint,
            200,                           # extract_rates a déjà validé le statut HTTP
            context["dag"].dag_id,
            dag_run.run_id,
            dag_run.logical_date,
        ),
    )[0]

    log.info(
        "[LOAD] %d paires ingérées dans fx.raw_exchange_rates — raw_id=%s | base=%s | run=%s",
        len(raw_data), raw_id, base_currency, dag_run.run_id,
    )
    return raw_id


# Les lignes sont validées en amont par quality_check (validation pure)
# rate/rate_date arrivent en str via XCom
# vers NUMERIC/DATE à l'INSERT

INSERT_VALID_SQL = """
    INSERT INTO fx.exchange_rates
        (base_currency, quote_currency, rate, rate_date, raw_id, run_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (base_currency, quote_currency, rate_date) DO UPDATE
        SET rate      = EXCLUDED.rate,
            raw_id    = EXCLUDED.raw_id,
            run_id    = EXCLUDED.run_id,
            loaded_at = now();
"""

INSERT_REJECTED_SQL = """
    INSERT INTO fx.rejected_exchange_rates
        (base_currency, quote_currency, rate, rate_date, quality_dimension,
         rejection_reason, raw_record, raw_id, run_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s);
"""


@task(task_id="load_rates")
def load_rates(quality_result: dict) -> dict:
    """Persiste le résultat du contrôle qualité en une seule transaction.
    Les lignes valides vont dans fx.exchange_rates (1 par paire et par date),
    les rejets dans le cimetière fx.rejected_exchange_rates. 
    """
    valid = quality_result.get("valid_rows", [])
    rejected = quality_result.get("rejected_rows", [])

    valid_params = [
        (r["base_currency"], r["quote_currency"], r["rate"],
         r["rate_date"], r["raw_id"], r["run_id"])
        for r in valid
    ]
    rejected_params = [
        (r["base_currency"], r["quote_currency"], r["rate"], r["rate_date"],
         r["quality_dimension"], r["rejection_reason"], r["raw_record"],
         r["raw_id"], r["run_id"])
        for r in rejected
    ]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if valid_params:
                cur.executemany(INSERT_VALID_SQL, valid_params)
            if rejected_params:
                cur.executemany(INSERT_REJECTED_SQL, rejected_params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    log.info(
        "[LOAD] %d valide(s) → fx.exchange_rates | %d rejet(s) → cimetière",
        len(valid_params), len(rejected_params),
    )
    return {"inserted": len(valid_params), "rejected": len(rejected_params)}
