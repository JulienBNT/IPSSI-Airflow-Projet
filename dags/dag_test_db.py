"""
DAG de test BDD (Personne 2).

Injecte un lot de données volontairement mixtes (valides + invalides) pour valider
de bout en bout les 3 tables : la brute (raw_exchange_rates), la structurée
(exchange_rates) et le cimetière (rejected_exchange_rates).

Chaîne : generate_bad_data >> load_raw >> quality_check >> verify_db >> log_anomaly
- load_raw       : la réponse mixte est stockée en brut (JSONB).
- quality_check  : route les valides vers la structurée, les invalides vers le cimetière.
- verify_db      : relit les 3 tables pour CE run et logue les compteurs.
- log_anomaly    : détecte l'anomalie (lignes rejetées > 0) => la tâche ÉCHOUE.

Déclenchement manuel uniquement (schedule=None). Le run finit en `failed` : c'est
le comportement attendu (chemin d'échec déclenché par l'anomalie).
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

# Rendre le package exchange_rates importable
sys.path.insert(0, os.path.dirname(__file__))

from exchange_rates.load import load_raw, POSTGRES_CONN_ID
from exchange_rates.quality import quality_check
from exchange_rates.lifecycle import log_anomaly

log = logging.getLogger(__name__)


@dag(
    dag_id="test_db_with_bad_data",
    description="Test BDD Personne 2 : brute + structurée + cimetière, avec détection d'anomalie",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["test", "bdd", "personne2"],
    default_args={"owner": "personne2", "retries": 0},
)
def test_db_pipeline():

    @task(task_id="generate_bad_data")
    def generate_bad_data() -> list:
        """Lot mixte : 3 lignes valides + 7 invalides (une par dimension qualité).

        Base 'USD' pour les valides afin de NE PAS écraser les données réelles
        (pipeline de prod en base EUR) via l'upsert (base, quote, date).
        """
        today = date.today().isoformat()
        return [
            # --- 3 lignes valides ---
            {"date": today, "base": "USD", "quote": "EUR", "rate": 0.92},
            {"date": today, "base": "USD", "quote": "GBP", "rate": 0.79},
            {"date": today, "base": "USD", "quote": "JPY", "rate": 156.3},
            # --- 7 lignes invalides (cimetière) ---
            {"date": today, "base": "USD", "quote": "CHF"},                     # complétude : rate manquant
            {"date": today, "base": "US",  "quote": "CAD", "rate": 1.36},       # structure  : base non ISO
            {"date": today, "base": "USD", "quote": "AUD", "rate": "abc"},      # structure  : rate non numérique
            {"date": today, "base": "USD", "quote": "SEK", "rate": -2.0},       # cohérence  : taux négatif
            {"date": today, "base": "USD", "quote": "USD", "rate": 1.0},        # cohérence  : base == quote
            {"date": "2020-01-01", "base": "USD", "quote": "NOK", "rate": 9.9}, # fraîcheur  : date périmée
            {"date": today, "base": "USD", "quote": "EUR", "rate": 0.93},       # unicité    : doublon USD/EUR
        ]

    @task(task_id="verify_db")
    def verify_db() -> dict:
        """Relit les 3 tables pour le run courant et logue les compteurs."""
        run_id = get_current_context()["run_id"]
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        raw_n = hook.get_first(
            "SELECT count(*) FROM fx.raw_exchange_rates WHERE run_id = %s", parameters=(run_id,)
        )[0]
        valid_n = hook.get_first(
            "SELECT count(*) FROM fx.exchange_rates WHERE run_id = %s", parameters=(run_id,)
        )[0]
        rejected_by_dim = hook.get_records(
            """
            SELECT quality_dimension, count(*)
            FROM fx.rejected_exchange_rates
            WHERE run_id = %s
            GROUP BY quality_dimension
            ORDER BY quality_dimension
            """,
            parameters=(run_id,),
        )
        rejected_n = sum(int(c) for _, c in rejected_by_dim)

        log.info(
            "[TEST-BDD] run=%s | brute=%d ligne(s) | structurée=%d valide(s) | "
            "cimetière=%d rejet(s) %s",
            run_id, raw_n, valid_n, rejected_n,
            {dim: int(c) for dim, c in rejected_by_dim},
        )
        # Garde-fous : le lot doit produire des valides ET des rejets
        assert raw_n >= 1, "La table brute devrait contenir le payload du run"
        assert valid_n >= 1, "La table structurée devrait contenir les lignes valides"
        assert rejected_n >= 1, "Le cimetière devrait contenir les lignes rejetées"

        return {"raw": raw_n, "valid": valid_n, "rejected": rejected_n}

    batch = generate_bad_data()
    raw_id = load_raw(batch)
    quality = quality_check(batch, raw_id)
    verify = verify_db()
    anomaly = log_anomaly()

    batch >> raw_id >> quality >> verify >> anomaly


test_db_pipeline()
