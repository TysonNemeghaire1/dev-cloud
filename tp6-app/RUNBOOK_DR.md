# Runbook Disaster Recovery — Bascule GCP → AWS

## Critères de déclenchement
- [ ] GCP région europe-west9 indisponible > 10 min (status.cloud.google.com)
- [ ] Health check Global LB échoue depuis > 5 min
- [ ] Décision validée par : SRE Lead + CTO

## Étapes (RTO cible : 15 min)

### T+0min : Activation
1. Page SRE on-call (PagerDuty)
2. Notifier #incident-war-room sur Slack
3. Démarrer le bridge call

### T+2min : Vérification cluster DR AWS

    kubectl --context=eks-fraudguard-dr get nodes
    kubectl --context=eks-fraudguard-dr get pods -n fraudguard
    # Le cluster EKS tourne en "warm standby" avec 0 réplica fraud-detector

### T+5min : Scale-up des workloads

    # ArgoCD déjà installé sur EKS → modifier le repo Git
    git checkout -b dr-failover
    sed -i 's/replicas: 0/replicas: 4/g' manifests/business/*/rollout.yaml
    git commit -am "DR: scale-up workloads on AWS"
    git push origin dr-failover

    # Merger immédiatement (skip review en mode incident)
    gh pr create --base main --head dr-failover --title "[DR] Failover GCP→AWS"
    gh pr merge --merge --auto

### T+10min : Bascule DNS

    # Modifier le record DNS pour pointer vers le LB AWS
    aws route53 change-resource-record-sets \
      --hosted-zone-id Z123ABC \
      --change-batch file://dr-failover-dns.json
    # TTL configuré à 60s → propagation rapide

### T+15min : Vérification SLO
- Latence P99 < 500ms : à confirmer dans Grafana (datasource AWS)
- Taux de succès > 99% : à confirmer
- Alertes traitées : vérifier que `alert-handler` consomme bien `fraud-alerts`
