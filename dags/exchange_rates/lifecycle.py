"""
lifecycle.py: Analytics Engineer & Monitoring
Gestion du cycle de vie du run : démarrage, anomalies, bilan final.
Écrit dans fx.ingestion_logs (1 ligne par run, idempotent).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import psycopg2
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.sdk import get_current_context, task

log = logging.getLogger(__name__)

_CONN_ID = "fx_postgres"


def _get_pg_conn():
    info = BaseHook.get_connection(_CONN_ID)
    return psycopg2.connect(
        host=info.host,
        port=info.port or 5432,
        dbname=info.schema,
        user=info.login,
        password=info.password,
    )


def on_task_failure(context: dict) -> None:
    """Callback Airflow appelé sur l'échec de n'importe quelle tâche."""
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown"
    ti = context.get("task_instance") or context.get("ti")
    task_id = ti.task_id if ti else "unknown"
    log.error("[lifecycle] Échec tâche '%s' — run_id=%s", task_id, run_id)


@task(task_id="log_start")
def log_start() -> dict:
    """Trace le démarrage du run (point d'entrée du pipeline)."""
    ctx = get_current_context()
    run_id = ctx["run_id"]
    log.info("[lifecycle] Démarrage run_id=%s", run_id)
    return {"run_id": run_id}


@task(task_id="log_anomaly", trigger_rule="all_done", retries=0)
def log_anomaly() -> None:
    """
    Aucun rejet  => AirflowSkipException (chemin nominal, tâche skippée).
    >= 1 rejet   => AirflowException (chemin d'échec, le run est marqué failed).
    """
    ctx = get_current_context()
    run_id = ctx["run_id"]
    ti = ctx["ti"]

    quality_result = ti.xcom_pull(task_ids="quality_check") or {}
    rejected = quality_result.get("rejected", 0)

    if not rejected:
        log.info("[lifecycle] run=%s : aucune anomalie détectée", run_id)
        raise AirflowSkipException("Aucune anomalie — tâche skippée")

    log.error("[lifecycle] run=%s : %d ligne(s) rejetée(s) — anomalie qualité", run_id, rejected)
    raise AirflowException(f"Anomalie qualité : {rejected} ligne(s) rejetée(s) (run {run_id})")


@task(task_id="log_end", trigger_rule="all_done")
def log_end() -> dict:
    """Compile les compteurs du run et écrit le bilan dans fx.ingestion_logs."""
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    run_id = dag_run.run_id
    # logical_date est None/absent pour un run manuel => fallback sur l'heure courante
    execution_date = dag_run.logical_date or datetime.now(timezone.utc)
    ti = ctx["ti"]

    quality_result = ti.xcom_pull(task_ids="quality_check") or {}

    # Clés renvoyées par quality_check (Personne 3) : received / valid / rejected / inserted
    lignes_recues   = quality_result.get("received", 0)
    lignes_valides  = quality_result.get("valid",    0)
    lignes_rejetees = quality_result.get("rejected", 0)
    lignes_inserees = quality_result.get("inserted", lignes_valides)

    if lignes_rejetees == 0 and lignes_valides > 0:
        status = "success"
    elif lignes_valides > 0:
        status = "partial"
    else:
        status = "failed"

    log.info(
        "[lifecycle] run=%s status=%s recues=%d valides=%d rejetees=%d inserees=%d",
        run_id, status, lignes_recues, lignes_valides, lignes_rejetees, lignes_inserees,
    )

    with _get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fx.ingestion_logs
                (run_id, execution_date, status,
                 lignes_recues, lignes_valides, lignes_rejetees, lignes_inserees)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                status          = EXCLUDED.status,
                lignes_recues   = EXCLUDED.lignes_recues,
                lignes_valides  = EXCLUDED.lignes_valides,
                lignes_rejetees = EXCLUDED.lignes_rejetees,
                lignes_inserees = EXCLUDED.lignes_inserees,
                logged_at       = now();
            """,
            (
                run_id,
                execution_date,
                status,
                lignes_recues,
                lignes_valides,
                lignes_rejetees,
                lignes_inserees,
            ),
        )

    return {"run_id": run_id, "status": status}
