"""
DAG : FraudGuard Daily Fraud Report
Génère un rapport quotidien de fraude pour chaque type d'alerte.
Utilise le Dynamic Task Mapping pour paralléliser l'analyse par type.

Adaptations vs énoncé (pour un run réellement vert sur notre projet) :
- project_id réel via la Variable Airflow `gcp_project_id` (= ynov-cloud-tyson).
- BigQuery : table `<project>.reporting.daily_fraud_summary` (créée côté infra).
- Réentraînement ML : KubernetesPodOperator `in_cluster=True` (Airflow tourne DANS
  le cluster GKE → pas besoin de credentials cross-cluster comme GKEStartPodOperator).
  C'est exactement l'opérateur évoqué dans la question 2.1 de l'énoncé.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

GCP_PROJECT = Variable.get('gcp_project_id', default_var='ynov-cloud-tyson')

ALERT_TYPES = [
    'MICRO_TRANSACTION_PATTERN',
    'HIGH_VELOCITY',
    'SUSPICIOUS_IP',
]

with DAG(
    dag_id='fraudguard_daily_report',
    description='Rapport quotidien de fraude FraudGuard — MIF II & ACPR',
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email': ['fraud-ops@fraudguard.fr'],
    },
    start_date=datetime(2026, 1, 1),
    schedule='0 7 * * *',   # 07h00 chaque matin
    catchup=False,
    tags=['fraud', 'reporting', 'acpr'],
) as dag:

    # ============================================================
    # Tâche 1 : Récupérer la liste des types d'alertes depuis Firestore
    # (En prod : requête Firestore pour les types actifs du jour)
    # ============================================================
    def get_alert_types_for_date(**context):
        """Retourne la liste des types d'alertes à analyser pour hier."""
        execution_date = context['ds']
        print(f"[FraudGuard] Récupération des types d'alertes pour {execution_date}")
        return ALERT_TYPES

    fetch_alert_types = PythonOperator(
        task_id='fetch_alert_types',
        python_callable=get_alert_types_for_date,
    )

    # ============================================================
    # Tâche 2 : Analyser chaque type d'alerte EN PARALLÈLE
    # Dynamic Task Mapping : une tâche par type d'alerte
    # ============================================================
    def analyze_alert_type(alert_type: str, **context):
        """Analyser les statistiques d'un type d'alerte spécifique."""
        execution_date = context['ds']
        print(f"[FraudGuard] Analyse {alert_type} pour {execution_date}")

        # Simulation de l'analyse (en prod : requête BigQuery)
        import random
        stats = {
            'alert_type': alert_type,
            'date': execution_date,
            'count': random.randint(5, 200),
            'unique_accounts_affected': random.randint(2, 50),
            'total_amount_at_risk': round(random.uniform(100, 50000), 2),
            'avg_detection_latency_ms': random.randint(50, 500),
            'false_positive_rate': round(random.uniform(0.02, 0.15), 3),
        }

        print(f"[FraudGuard] {alert_type}: {stats['count']} alertes, "
              f"{stats['unique_accounts_affected']} comptes, "
              f"{stats['total_amount_at_risk']}€ à risque")
        return stats

    # Dynamic Task Mapping : .expand() génère N tâches en parallèle
    analyze_by_type = PythonOperator.partial(
        task_id='analyze_alert_type',
        python_callable=analyze_alert_type,
    ).expand(
        op_kwargs=[{'alert_type': t} for t in ALERT_TYPES]
    )

    # ============================================================
    # Tâche 3 : Consolider les analyses de tous les types
    # ============================================================
    def consolidate_report(**context):
        """Agréger les analyses de tous les types d'alertes en un rapport global."""
        # list(...) : matérialise la séquence lazy du Dynamic Task Mapping
        # (LazyXComSelectSequence non sérialisable en JSON si on la stocke telle quelle).
        all_stats = list(context['ti'].xcom_pull(task_ids='analyze_alert_type'))

        if not all_stats:
            print("[FraudGuard] Aucune donnée à consolider")
            return

        total_alerts = sum(s['count'] for s in all_stats)
        total_amount = sum(s['total_amount_at_risk'] for s in all_stats)

        report = {
            'date': context['ds'],
            'total_alerts': total_alerts,
            'total_amount_at_risk': round(total_amount, 2),
            'breakdown_by_type': all_stats,
            'generated_at': datetime.now().isoformat(),
            'regulatory_compliant': True,
        }

        print(f"\n{'='*50}")
        print(f"RAPPORT FRAUDE FraudGuard — {context['ds']}")
        print(f"Total alertes : {total_alerts}")
        print(f"Montant à risque : {total_amount:.2f}€")
        print(f"{'='*50}\n")

        context['ti'].xcom_push(key='daily_report', value=report)
        return report

    consolidate = PythonOperator(
        task_id='consolidate_report',
        python_callable=consolidate_report,
    )

    # ============================================================
    # Tâche 4 : Charger le rapport dans BigQuery
    # ============================================================
    load_report_bq = BigQueryInsertJobOperator(
        task_id='load_report_to_bigquery',
        configuration={
            'query': {
                'query': f"""
                    INSERT INTO `{GCP_PROJECT}.reporting.daily_fraud_summary`
                    VALUES (
                        '{{{{ ds }}}}',
                        {{{{ ti.xcom_pull(task_ids='consolidate_report', key='daily_report')['total_alerts'] }}}},
                        {{{{ ti.xcom_pull(task_ids='consolidate_report', key='daily_report')['total_amount_at_risk'] }}}},
                        CURRENT_TIMESTAMP()
                    )
                """,
                'useLegacySql': False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    # ============================================================
    # Tâche 5 : Réentraîner le modèle ML si le taux de faux positifs est trop élevé
    # KubernetesPodOperator : lance un pod dédié avec ses propres ressources
    # ============================================================
    def check_should_retrain(**context):
        """Vérifier si les faux positifs justifient un réentraînement du modèle."""
        all_stats = list(context['ti'].xcom_pull(task_ids='analyze_alert_type'))
        avg_fp_rate = sum(s['false_positive_rate'] for s in all_stats) / len(all_stats)
        print(f"[FraudGuard] Taux de faux positifs moyen : {avg_fp_rate:.2%}")
        should_retrain = avg_fp_rate > 0.10
        context['ti'].xcom_push(key='should_retrain', value=should_retrain)
        return should_retrain

    check_retrain = PythonOperator(
        task_id='check_should_retrain',
        python_callable=check_should_retrain,
    )

    # Lance un pod Kubernetes dédié pour le réentraînement du modèle.
    # in_cluster=True : Airflow s'exécute dans le cluster → utilise le SA in-cluster.
    retrain_model = KubernetesPodOperator(
        task_id='retrain_fraud_model',
        namespace='fraudguard',
        image=f'europe-west9-docker.pkg.dev/{GCP_PROJECT}/tp2-registry/fraud-ml-trainer:latest',
        # name : pas de template Jinja ici (validé au parsing) ; un suffixe aléatoire
        # est ajouté automatiquement (random_name_suffix=True par défaut).
        name='fraud-model-retrain',
        arguments=[
            '--training-date', '{{ ds }}',
            '--model-output', 'gs://fraudguard-models/fraud-detector-{{ ds_nodash }}',
        ],
        # NB : l'énoncé illustre 8Gi/2 CPU (et 2 GPU). Réduit ici pour rester
        # schedulable sur le cluster de TP (quota SSD/CPU limité) sans déclencher
        # un nouveau nœud. En prod : remettre les valeurs cibles + node pool GPU.
        container_resources={
            'requests': {'memory': '256Mi', 'cpu': '200m'},
            'limits': {'memory': '512Mi', 'cpu': '500m'},
        },
        in_cluster=True,
        get_logs=True,
    )

    # ============================================================
    # Tâche 6 : Détecter un jour anormalement frauduleux (déclenche investigation)
    # ============================================================
    def check_anomalous_day(**context):
        """Détecter si ce jour est anormalement frauduleux."""
        report = context['ti'].xcom_pull(task_ids='consolidate_report', key='daily_report')
        # Seuil : > 500 alertes en une journée = anormal
        is_anomalous = report['total_alerts'] > 500
        context['ti'].xcom_push(key='is_anomalous', value=is_anomalous)
        if is_anomalous:
            print(f"[ALERTE] Jour anormal détecté : {report['total_alerts']} alertes !")
        return is_anomalous

    check_anomaly = PythonOperator(
        task_id='check_anomalous_day',
        python_callable=check_anomalous_day,
    )

    # Déclencher le DAG d'investigation si la journée est anormale
    trigger_investigation = TriggerDagRunOperator(
        task_id='trigger_investigation',
        trigger_dag_id='fraudguard_deep_investigation',
        conf={
            'triggered_by': 'daily_report',
            'trigger_date': '{{ ds }}',
            'alert_count': "{{ ti.xcom_pull(task_ids='consolidate_report', key='daily_report')['total_alerts'] }}",
        },
        wait_for_completion=False,   # Ne pas bloquer le rapport en attendant l'investigation
    )

    # ============================================================
    # Ordre des tâches (Dynamic Task Mapping + branches)
    # ============================================================
    fetch_alert_types >> analyze_by_type >> consolidate >> [load_report_bq, check_retrain]
    check_retrain >> retrain_model
    consolidate >> check_anomaly >> trigger_investigation
