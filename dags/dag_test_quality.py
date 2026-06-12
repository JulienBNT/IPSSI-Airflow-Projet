"""
DAG de test pour valider le module quality.py avec des donnees invalides
Ce DAG simule des appels API qui renvoient des donnees corrompues
"""
from __future__ import annotations

import sys
import os
from datetime import datetime
from airflow.sdk import dag, task

# Ajouter le dossier dags au PYTHONPATH
sys.path.insert(0, os.path.dirname(__file__))

from exchange_rates.quality import quality_check
from exchange_rates.load import load_raw


@dag(
    dag_id="test_quality_with_bad_data",
    description="Test du module quality avec des donnees invalides",
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["test", "quality"],
    default_args={
        "owner": "damien",
        "retries": 0,
    }
)
def test_quality_pipeline():
    """DAG de test pour injecter des donnees invalides et verifier le cimetiere"""
    
    @task(task_id="generate_bad_data_scenario_1")
    def generate_completude_errors():
        """Scenario 1: Erreurs de completude (champs manquants ou None)"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EUR", "quote": "GBP"},
            {"date": "2026-06-11", "base": "EUR", "quote": "JPY", "rate": None},
            {"base": "EUR", "quote": "CHF", "rate": 1.07},
        ]
    
    @task(task_id="generate_bad_data_scenario_2")
    def generate_structure_errors():
        """Scenario 2: Erreurs de structure (formats invalides)"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EU", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EUR", "quote": "US$", "rate": 1.08},
            {"date": "2026/06/11", "base": "EUR", "quote": "GBP", "rate": 0.85},
            {"date": "2026-06-11", "base": "EUR", "quote": "JPY", "rate": "abc"},
        ]
    
    @task(task_id="generate_bad_data_scenario_3")
    def generate_coherence_errors():
        """Scenario 3: Erreurs de coherence (regles metier violees)"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EUR", "quote": "GBP", "rate": 0},
            {"date": "2026-06-11", "base": "EUR", "quote": "JPY", "rate": -150.0},
            {"date": "2026-06-11", "base": "EUR", "quote": "EUR", "rate": 1.0},
        ]
    
    @task(task_id="generate_bad_data_scenario_4")
    def generate_fraicheur_errors():
        """Scenario 4: Erreurs de fraicheur (dates perimees ou futures)"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2020-01-01", "base": "EUR", "quote": "GBP", "rate": 0.85},
            {"date": "2026-06-01", "base": "EUR", "quote": "JPY", "rate": 150.0},
            {"date": "2026-06-15", "base": "EUR", "quote": "CHF", "rate": 1.07},
        ]
    
    @task(task_id="generate_bad_data_scenario_5")
    def generate_unicite_errors():
        """Scenario 5: Erreurs d'unicite (doublons)"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EUR", "quote": "GBP", "rate": 0.85},
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.09},
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.10},
        ]
    
    @task(task_id="generate_bad_data_scenario_6")
    def generate_mixed_errors():
        """Scenario 6: Mix de plusieurs types d'erreurs"""
        return [
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2026-06-11", "base": "EU", "quote": "GBP", "rate": 0.85},
            {"base": "EUR", "quote": "JPY", "rate": 150.0},
            {"date": "2026-06-11", "base": "EUR", "quote": "CHF", "rate": -1.07},
            {"date": "2020-01-01", "base": "EUR", "quote": "CAD", "rate": 1.5},
            {"date": "2026-06-11", "base": "EUR", "quote": "USD", "rate": 1.09},
        ]
    
    scenario_1 = generate_completude_errors()
    scenario_2 = generate_structure_errors()
    scenario_3 = generate_coherence_errors()
    scenario_4 = generate_fraicheur_errors()
    scenario_5 = generate_unicite_errors()
    scenario_6 = generate_mixed_errors()
    
    raw_id_1 = load_raw(scenario_1)
    raw_id_2 = load_raw(scenario_2)
    raw_id_3 = load_raw(scenario_3)
    raw_id_4 = load_raw(scenario_4)
    raw_id_5 = load_raw(scenario_5)
    raw_id_6 = load_raw(scenario_6)
    
    quality_1 = quality_check(scenario_1, raw_id_1)
    quality_2 = quality_check(scenario_2, raw_id_2)
    quality_3 = quality_check(scenario_3, raw_id_3)
    quality_4 = quality_check(scenario_4, raw_id_4)
    quality_5 = quality_check(scenario_5, raw_id_5)
    quality_6 = quality_check(scenario_6, raw_id_6)
    
    @task(task_id="summary")
    def print_summary(q1, q2, q3, q4, q5, q6):
        """Affiche un resume des tests"""
        total_valid = sum([q["valid"] for q in [q1, q2, q3, q4, q5, q6]])
        total_rejected = sum([q["rejected"] for q in [q1, q2, q3, q4, q5, q6]])
        return {"total_valid": total_valid, "total_rejected": total_rejected}
    
    summary = print_summary(quality_1, quality_2, quality_3, quality_4, quality_5, quality_6)


test_quality_pipeline()
