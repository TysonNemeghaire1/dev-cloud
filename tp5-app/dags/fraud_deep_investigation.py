"""
DAG : FraudGuard Deep Investigation
Déclenché par `fraudguard_daily_report` (via TriggerDagRunOperator) quand une
journée est jugée anormalement frauduleuse (> 500 alertes).

Investigation approfondie post-hoc : reçoit le contexte (date, nombre d'alertes)
dans `dag_run.conf` et déroule une analyse renforcée.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='fraudguard_deep_investigation',
    description='Investigation approfondie déclenchée sur jour anormal',
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    start_date=datetime(2026, 1, 1),
    schedule=None,   # Déclenché uniquement par le daily_report
    catchup=False,
    tags=['fraud', 'investigation'],
) as dag:

    def receive_trigger_context(**context):
        """Lire le contexte transmis par le DAG déclencheur."""
        conf = context['dag_run'].conf or {}
        triggered_by = conf.get('triggered_by', 'manual')
        trigger_date = conf.get('trigger_date', 'n/a')
        alert_count = conf.get('alert_count', 'n/a')
        print(f"[INVESTIGATION] Déclenchée par : {triggered_by}")
        print(f"[INVESTIGATION] Date analysée : {trigger_date} | Alertes : {alert_count}")
        return {'trigger_date': trigger_date, 'alert_count': alert_count}

    receive_context = PythonOperator(
        task_id='receive_trigger_context',
        python_callable=receive_trigger_context,
    )

    def analyze_account_graph(**context):
        """Analyse de graphe : détecter les réseaux de comptes mules."""
        print("[INVESTIGATION] Analyse du graphe de comptes (détection de mules)…")
        # En prod : requête sur les liens compte→compte, clustering
        suspected_rings = ['ACC-9999', 'ACC-MULE-002', 'ACC-MULE-003']
        print(f"[INVESTIGATION] Réseaux suspects identifiés : {suspected_rings}")
        return suspected_rings

    account_graph = PythonOperator(
        task_id='analyze_account_graph',
        python_callable=analyze_account_graph,
    )

    def escalate_to_compliance(**context):
        """Escalader le dossier vers la conformité / ACPR."""
        print("[INVESTIGATION] Dossier escaladé vers l'équipe Conformité (ACPR).")

    escalate = PythonOperator(
        task_id='escalate_to_compliance',
        python_callable=escalate_to_compliance,
    )

    receive_context >> account_graph >> escalate
