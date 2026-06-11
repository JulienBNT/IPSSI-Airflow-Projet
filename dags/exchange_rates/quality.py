from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task, Variable

from exchange_rates.config import FRESHNESS_DAYS_DEFAULT

log = logging.getLogger(__name__)

# Connection ID Airflow vers PostgreSQL (Admin > Connections) — partagé avec load.py
POSTGRES_CONN_ID = "fx_postgres"

# Dimensions de qualité contrôlées (alignées sur la contrainte CHECK de init_db.sql)
DIM_COMPLETUDE = "completude"
DIM_STRUCTURE = "structure"
DIM_COHERENCE = "coherence"
DIM_FRAICHEUR = "fraicheur"
DIM_UNICITE = "unicite"

# Codes ISO 4217 : 3 lettres majuscules
ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Chargement idempotent : rejouer un run met à jour la ligne sans créer de doublon.
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


def _as_text(value) -> str | None:
    """Représentation texte non destructive (conserve la valeur reçue pour le cimetière)."""
    if value is None:
        return None
    return str(value)


def _validate_row(item, seen_keys: set, run_date: date, freshness_days: int):
    """Applique les 5 dimensions de qualité à une ligne.

    Retourne (dimension, motif) au premier échec, ou None si la ligne est valide.
    L'ordre des contrôles va du plus structurel au plus métier.
    """
    # 1. STRUCTURE — l'enregistrement doit être un objet exploitable
    if not isinstance(item, dict):
        return DIM_STRUCTURE, f"Enregistrement non structuré (type={type(item).__name__})"

    required = ("date", "base", "quote", "rate")

    # 2. COMPLÉTUDE — tous les champs requis présents et non vides
    for field in required:
        value = item.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return DIM_COMPLETUDE, f"Champ obligatoire manquant ou vide : '{field}'"

    base = str(item["base"]).strip().upper()
    quote = str(item["quote"]).strip().upper()

    # 3. STRUCTURE (format) — codes ISO 4217, taux numérique, date ISO valide
    if not ISO_CURRENCY_RE.match(base):
        return DIM_STRUCTURE, f"Code devise de base invalide : '{item['base']}' (attendu ISO 4217)"
    if not ISO_CURRENCY_RE.match(quote):
        return DIM_STRUCTURE, f"Code devise cible invalide : '{item['quote']}' (attendu ISO 4217)"

    try:
        rate = Decimal(str(item["rate"]))
        # Vérifier que le taux n'est pas NaN ou Infinity
        if not rate.is_finite():
            return DIM_STRUCTURE, f"Taux invalide (NaN ou Infinity) : '{item['rate']}'"
    except (InvalidOperation, ValueError, TypeError):
        return DIM_STRUCTURE, f"Taux non numérique : '{item['rate']}'"

    try:
        rate_date = datetime.strptime(str(item["date"]).strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return DIM_STRUCTURE, f"Date invalide : '{item['date']}' (attendu AAAA-MM-JJ)"

    # 4. COHÉRENCE — règles métier (taux strictement positif, devises distinctes)
    if rate <= 0:
        return DIM_COHERENCE, f"Taux non positif : {rate}"
    if base == quote:
        return DIM_COHERENCE, f"Devise de base et cible identiques : {base}"

    # 5. FRAÎCHEUR — la date du taux ne doit pas être trop ancienne ni dans le futur
    age_days = (run_date - rate_date).days
    if age_days > freshness_days:
        return DIM_FRAICHEUR, (
            f"Taux périmé : date={rate_date} ({age_days}j > seuil {freshness_days}j)"
        )
    if age_days < 0:
        return DIM_FRAICHEUR, f"Date dans le futur : {rate_date} (run={run_date})"

    # 6. UNICITÉ — pas de doublon (base, quote, date) dans le lot
    key = (base, quote, rate_date)
    if key in seen_keys:
        return DIM_UNICITE, f"Doublon dans le lot : {base}/{quote} @ {rate_date}"
    seen_keys.add(key)

    return None


@task(task_id="quality_check")
def quality_check(raw_data: list, raw_id: int, **context) -> dict:
    """Contrôle qualité (Personne 3).

    Applique 5 dimensions de qualité (complétude, structure, cohérence,
    fraîcheur, unicité) à chaque paire de devises. Les lignes valides sont
    chargées dans fx.exchange_rates (idempotent), les lignes invalides sont
    tracées et chargées dans le cimetière fx.rejected_exchange_rates.
    """
    if not raw_data:
        raise ValueError("Aucune donnée à contrôler")

    dag_run = context["dag_run"]
    run_id = dag_run.run_id
    logical_date = dag_run.logical_date or datetime.now(timezone.utc)
    run_date = logical_date.date()

    freshness_days = int(
        Variable.get("exchange_rate_freshness_days", default=str(FRESHNESS_DAYS_DEFAULT))
    )

    seen_keys: set = set()
    valid_rows: list[tuple] = []
    rejected_rows: list[tuple] = []

    for item in raw_data:
        failure = _validate_row(item, seen_keys, run_date, freshness_days)

        if failure is None:
            base = str(item["base"]).strip().upper()
            quote = str(item["quote"]).strip().upper()
            rate = Decimal(str(item["rate"]))
            rate_date = datetime.strptime(str(item["date"]).strip(), "%Y-%m-%d").date()
            valid_rows.append((base, quote, rate, rate_date, raw_id, run_id))
        else:
            dimension, reason = failure
            record = item if isinstance(item, dict) else {"value": item}
            rejected_rows.append(
                (
                    _as_text(record.get("base") if isinstance(record, dict) else None),
                    _as_text(record.get("quote") if isinstance(record, dict) else None),
                    _as_text(record.get("rate") if isinstance(record, dict) else None),
                    _as_text(record.get("date") if isinstance(record, dict) else None),
                    dimension,
                    reason,
                    json.dumps(record, default=str),
                    raw_id,
                    run_id,
                )
            )
            log.warning("[QUALITY][REJET] dimension=%s | %s", dimension, reason)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # executemany + ON CONFLICT pour un chargement idempotent des lignes valides
    if valid_rows:
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(INSERT_VALID_SQL, valid_rows)
        conn.commit()

    # traçage des lignes invalides dans le cimetière
    if rejected_rows:
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(INSERT_REJECTED_SQL, rejected_rows)
        conn.commit()

    summary = {
        "status": "success",
        "received": len(raw_data),
        "valid": len(valid_rows),
        "rejected": len(rejected_rows),
        "inserted": len(valid_rows),
        "run_id": run_id,
    }
    log.info(
        "[QUALITY] reçues=%(received)d | valides=%(valid)d | rejetées=%(rejected)d | "
        "insérées=%(inserted)d | seuil_fraicheur=%(fresh)dj | run=%(run)s",
        {
            "received": summary["received"],
            "valid": summary["valid"],
            "rejected": summary["rejected"],
            "inserted": summary["inserted"],
            "fresh": freshness_days,
            "run": run_id,
        },
    )
    return summary
