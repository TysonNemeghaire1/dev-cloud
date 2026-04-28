# LogiStream — Architecture GKE + Kafka

Plateforme de suivi de livraisons en temps réel pour flottes de camions.

---

## Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │                  GKE Autopilot                       │
                        │                Namespace: logistream                 │
                        │                                                       │
  Mobile App       ───► │  ┌─────────────────┐      ┌──────────────────────┐  │
  (Chauffeurs)          │  │   API Gateway    │      │   Tracker Service    │  │
                        │  │  (x2 replicas)   │      │  (x2-10 replicas)    │  │
  GPS 10s/camion        │  │  LoadBalancer IP │      │  HPA: 60% CPU        │  │
                        │  └────────┬────────┘      └──────────────────────┘  │
                        │           │                                           │
                        │  ┌────────▼────────┐      ┌──────────────────────┐  │
                        │  │   GPS Producer  │      │  Tracker Consumer    │  │
                        │  │  (x1 replica)   │      │   (x3 replicas)      │  │
                        │  └────────┬────────┘      └──────────┬───────────┘  │
                        │           │                           │               │
                        └───────────┼───────────────────────────┼──────────────┘
                                    │                           │
                        ┌───────────▼───────────────────────────▼──────────────┐
                        │              Namespace: kafka                          │
                        │                                                        │
                        │   ┌──────────────────────────────────────────────┐    │
                        │   │          Apache Kafka (Strimzi KRaft)         │    │
                        │   │                                               │    │
                        │   │  ┌─────────────┐  ┌──────────────────────┐  │    │
                        │   │  │ dual-role-0 │  │    truck-positions    │  │    │
                        │   │  │ dual-role-1 │  │    (6 partitions)     │  │    │
                        │   │  │ dual-role-2 │  │    delivery-alerts    │  │    │
                        │   │  │ (controller │  │    (3 partitions)     │  │    │
                        │   │  │  + broker)  │  │    delivery-events    │  │    │
                        │   │  └─────────────┘  │    (3 partitions)     │  │    │
                        │   │                   └──────────────────────┘  │    │
                        │   └──────────────────────────────────────────────┘    │
                        └────────────────────────────────────────────────────────┘

                        ┌────────────────────────────────────────────────────────┐
                        │              Google Cloud Platform                      │
                        │                                                         │
                        │  Artifact Registry    Cloud Logging    Cloud Monitoring │
                        │  (tp2-registry)       (logs pods)      (alertes mémoire)│
                        └────────────────────────────────────────────────────────┘

                        ┌────────────────────────────────────────────────────────┐
                        │              GitHub Actions CI/CD                       │
                        │                                                         │
                        │   test ──► build-push ──► deploy                       │
                        │   (npm)    (docker+AR)    (kubectl)                     │
                        └────────────────────────────────────────────────────────┘
```

---

## Flux de données

1. Le **GPS Producer** simule 3 camions et envoie des positions GPS toutes les 10s sur le topic `truck-positions`
2. Les **3 Tracker Consumers** consomment les positions en parallèle (2 partitions chacun, LAG = 0)
3. Le consumer détecte les anomalies (camion arrêté, carburant bas) et publie sur `delivery-alerts`
4. Le **HPA** scale automatiquement le Tracker Service entre 2 et 10 replicas selon le CPU

---

## Stack technique

| Composant | Technologie |
|-----------|-------------|
| Orchestration | GKE Autopilot (europe-west9) |
| Streaming | Apache Kafka 4.2.0 via Strimzi (KRaft) |
| Producer/Consumer | Node.js 20 + KafkaJS |
| CI/CD | GitHub Actions (test → build → deploy) |
| Registry | Artifact Registry (europe-west9) |
| Observabilité | Cloud Logging + Cloud Monitoring |

---

## Deploiement

```bash
# 1. Appliquer les manifests Kubernetes
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/api-gateway.yaml
kubectl apply -f k8s/tracker-service.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f k8s/kafka-apps.yaml

# 2. Deployer le cluster Kafka
kubectl apply -f kafka/cluster.yaml
kubectl apply -f kafka/topics.yaml

# 3. Verifier
kubectl get pods -n logistream
kubectl get pods -n kafka
kubectl get kafkatopics -n kafka
```
