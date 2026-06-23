# FraudGuard — GitOps, Observabilité SRE & Multi-cloud

Dépôt GitOps (source de vérité) du pipeline de détection de fraude FraudGuard.
Tout l'environnement (infra + applicatif) est déployé par **ArgoCD** via le pattern
**App of Apps**. Aucun `kubectl apply` manuel en production : le seul appliqué une
fois est `bootstrap/root-app.yaml`.

## Architecture GitOps

```
                        ┌─────────────────────────┐
                        │  Git (dev-cloud/tp6-app) │  ← source de vérité
                        └────────────┬────────────┘
                                     │ pull (réconciliation 3 min)
                                     ▼
                        ┌─────────────────────────┐
                        │   ArgoCD (namespace argocd)│
                        │   App: fraudguard-root    │
                        └────────────┬────────────┘
                       App of Apps (recurse: true)
        ┌────────────────────────────┼────────────────────────────┐
        ▼ wave 0                      ▼ wave 0/1                    ▼ wave 2
  ┌───────────────┐          ┌──────────────────┐         ┌──────────────────┐
  │ argo-rollouts │          │ kafka (Strimzi)  │         │ tx-producer      │
  │ chaos-mesh    │          │ monitoring (kps) │         │ fraud-detector   │
  └───────────────┘          │ loki / promtail  │         │ alert-handler    │
                             │ tempo            │         └──────────────────┘
                             └──────────────────┘
```

### Sync waves
| Wave | Composants | Raison |
|---|---|---|
| 0  | argo-rollouts, kafka, monitoring, loki, tempo, chaos-mesh | Plateforme et CRD (Rollout, PrometheusRule, etc.) — installés avant l'applicatif |
| 1  | promtail | Dépend de Loki |
| 2  | tx-producer, fraud-detector, alert-handler | Applicatif métier, dépend de Kafka et du CRD Rollout |

> Les **recording rules SLO** (`manifests/infrastructure/monitoring/recording-rules.yaml`) sont
> découvertes par Prometheus via le label `release: monitoring` (chart kube-prometheus-stack).

## Stack d'observabilité (LGTM)
- **L**oki — agrégation de logs (chart `grafana/loki`, mode SingleBinary) + Promtail
- **G**rafana — dashboards (livré par kube-prometheus-stack)
- **T**empo — tracing distribué (OTLP gRPC `:4317`), instrumentation OpenTelemetry
- **M**étriques — Prometheus (kube-prometheus-stack) + recording rules SLO / Error Budget

## Progressive Delivery
`fraud-detector` est une ressource **Argo Rollouts** `Rollout` (Canary 20 → 40 → 80 → 100%)
avec analyse Prometheus automatique (`AnalysisTemplate`) à chaque palier → rollback auto
si le taux de succès du canary < 99%.

## Architecture cible Multi-cloud (DR GCP ↔ AWS)

Objectif : **RTO < 15 min** vers AWS en cas de désastre GCP (warm standby, pas actif/actif).

| Service GCP (Primaire) | Équivalent AWS (DR) | Mécanisme de synchronisation |
|---|---|---|
| GKE (cluster fraudguard) | **EKS** | Manifestes identiques via GitOps |
| Cloud Storage (modèles ML) | **S3** | Sync via Storage Transfer Service |
| Firestore (alertes) | **DynamoDB** | CDC via Change Data Capture |
| Artifact Registry (images) | **ECR** | Mirror via Skopeo cron job |
| Cloud DNS | **Route 53** | Failover record (TTL = 60s) |
| Cloud Load Balancing | **AWS Global Accelerator** | Global Accelerator avec health check |

Procédure de bascule détaillée : [`RUNBOOK_DR.md`](./RUNBOOK_DR.md).
Rapport de résilience : [`CHAOS_REPORT.md`](./CHAOS_REPORT.md).

## Bootstrap

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl wait --for=condition=Ready pods --all -n argocd --timeout=300s

# Le SEUL kubectl apply manuel : la Root App. Ensuite, tout passe par Git.
kubectl apply -f bootstrap/root-app.yaml
```

> **Note** : les commandes de création de cluster GCP (`gcloud container clusters ...`)
> ne sont pas exécutées dans ce contexte (pas de compte GCP actif). Les manifestes
> restent cloud-agnostiques et fonctionnent sur n'importe quel cluster Kubernetes.

## Arborescence

# Monorepo dev-cloud, contenu GitOps sous tp6-app/ (root-app pointe sur tp6-app/apps)
```
tp6-app/
├── bootstrap/root-app.yaml          # Root Application (App of Apps)
├── apps/                            # Applications ArgoCD (le "quoi déployer")
│   ├── infrastructure/{kafka,monitoring,argo-rollouts,chaos-mesh}/
│   └── business/{tx-producer,fraud-detector,alert-handler}-app.yaml
├── manifests/                       # Ressources Kubernetes (le "contenu")
│   ├── infrastructure/{kafka,monitoring,argo-rollouts}/
│   └── business/{tx-producer,fraud-detector,alert-handler}/
├── chaos/experiments/               # PodChaos, NetworkChaos, StressChaos
├── terraform/modules/fraudguard-cluster/   # Module multi-provider GCP/AWS
├── hpa.yaml                         # HPA fraud-detector (kubectl apply -f hpa.yaml)
├── CHAOS_REPORT.md
├── RUNBOOK_DR.md
└── README.md
```
