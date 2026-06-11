from __future__ import annotations

import logging

from airflow.sdk import task

log = logging.getLogger(__name__)


def on_task_failure(context: dict) -> None:
    """Callback déclenché à chaque tentative échouée (retry inclus)."""
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


@task(task_id="log_start")
def log_start() -> None:
    log.info("[START] Pipeline exchange_rates_pipeline démarré")


@task(task_id="log_anomaly", trigger_rule="one_failed")
def log_anomaly(**context) -> None:
    """S'exécute uniquement si une tâche upstream a définitivement échoué."""
    ti = context["task_instance"]
    log.error(
        "[ANOMALY] DAG=%s | run=%s | une tâche upstream a échoué — pipeline interrompu",
        ti.dag_id,
        ti.run_id,
    )


@task(task_id="log_end", trigger_rule="all_done")
def log_end() -> None:
    """S'exécute toujours, quel que soit le chemin emprunté."""
    log.info("[END] Pipeline exchange_rates_pipeline terminé")
