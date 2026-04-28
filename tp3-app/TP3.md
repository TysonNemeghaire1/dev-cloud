# TP 3 — Kubernetes (GKE), Apache Kafka & Pipeline CI/CD

**Cours 3 | Developper pour le Cloud | YNOV Campus Montpellier — Master 2**

Date : 28/04/2026 | Duree TP : 3h30 | Plateforme : Google Cloud Platform

---

## Livrables

| Livrable | Statut |
|----------|------|
| Microservices deployes sur GKE avec HPA fonctionnel |  |
| Cluster Kafka 3 brokers operationnel sur Kubernetes |  |
| Producer qui envoie des positions GPS et Consumer qui les traite en temps reel |  |
| Pipeline CI/CD avec 3 jobs reussis |  |
| README.md avec diagramme de l'architecture LogiStream |  |
| Observabilite (Cloud Logging, Monitoring, consumer lag) |  |

---

> **Contexte entreprise — LogiStream**
>
> LogiStream est une startup B2B qui fournit une plateforme de suivi de livraisons en temps reel a des transporteurs routiers. Leurs chauffeurs envoient leur position GPS toutes les 10 secondes depuis une application mobile. Avec 800 camions actifs simultanement, ca represente **80 evenements/seconde** en continu. L'architecture actuelle (un serveur Node.js + PostgreSQL) ne tient plus la charge : les positions arrivent avec 45 secondes de retard aux heures de pointe, rendant le suivi client inutilisable. La solution retenue : deployer les microservices sur **GKE** et implementer **Apache Kafka** comme backplane de streaming pour absorber les pics de charge.

---

## Partie 1 — Deploiement des microservices LogiStream sur GKE

> On deploie d'abord les microservices metier : un **API Gateway** (point d'entree HTTP) et un **Tracker Service** (traitement des positions GPS). Ces services communiqueront ensuite via Kafka.

### 1.1 — Creer le cluster GKE

```bash
# Creer un cluster GKE Autopilot (nodes geres automatiquement par GCP)
gcloud container clusters create-auto logistream-cluster \
  --region=europe-west9 \
  --project=$(gcloud config get-value project)
```

```
NAME                LOCATION      MASTER_VERSION      MASTER_IP       MACHINE_TYPE   NODE_VERSION        NUM_NODES  STATUS   STACK_TYPE
logistream-cluster  europe-west9  1.35.1-gke.1396002  34.155.142.176  ek-standard-8  1.35.1-gke.1396002  3          RUNNING  IPV4
```

```bash
# Configurer kubectl pour pointer sur ce cluster
gcloud container clusters get-credentials logistream-cluster \
  --region=europe-west9
```

```
Fetching cluster endpoint and auth data.
kubeconfig entry generated for logistream-cluster.
```

```bash
# Verifier la connexion au cluster
kubectl get nodes
```

```
No resources found
# Note : GKE Autopilot ne provisionne les nodes qu'a la demande (lors du deploiement de pods)
```

```bash
# Creer un namespace dedie a LogiStream
kubectl create namespace logistream

# Verifier la creation du namespace
kubectl get namespaces
```

```
namespace/logistream created

NAME                           STATUS   AGE
default                        Active   5m37s
gke-gmp-system                 Active   2m42s
gke-managed-cim                Active   3m55s
gke-managed-filestorecsi       Active   3m40s
gke-managed-parallelstorecsi   Active   2m9s
gke-managed-system             Active   3m14s
gke-managed-volumepopulator    Active   3m2s
gmp-public                     Active   2m42s
kube-node-lease                Active   5m37s
kube-public                    Active   5m37s
kube-system                    Active   5m37s
logistream                     Active   7s
```

### 1.2 — ConfigMap : configuration centralisee des services

Fichier `tp3-app/k8s/configmap.yaml` :

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: logistream-config
  namespace: logistream
data:
  APP_ENV: "production"
  LOG_LEVEL: "info"
  KAFKA_BOOTSTRAP_SERVERS: "kafka-cluster-kafka-bootstrap.logistream.svc.cluster.local:9092"
  KAFKA_TOPIC_POSITIONS: "truck-positions"
  KAFKA_TOPIC_ALERTS: "delivery-alerts"
  GPS_UPDATE_INTERVAL_MS: "10000"
  MAX_TRUCKS: "1000"
```

### 1.3 — Secret : credentials de base de donnees

Fichier `tp3-app/k8s/secret.yaml` :

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: logistream-secrets
  namespace: logistream
type: Opaque
stringData:
  DB_URL: "postgresql://logistream:motdepasse@cloud-sql-proxy:5432/logistream_prod"
  MAPS_API_KEY: "AIza-demo-logistream-maps-key-tp3"
  JWT_SECRET: "logistream-jwt-secret-2026-production"
```

> **Question :** Dans Kubernetes, un Secret de type `Opaque` encode les valeurs en base64 mais ne les chiffre pas au repos par defaut. Quelle configuration GKE permet de chiffrer les Secrets ETCD avec une cle Cloud KMS ? Pourquoi est-ce indispensable en production ?
>
> **Reponse :** GKE propose l'**Application-layer Secrets Encryption** via l'option `--database-encryption-key` a la creation du cluster, qui chiffre les Secrets etcd avec une cle AES-256 geree par Cloud KMS. C'est indispensable en production car sans cela, tout acces direct a etcd (backup compromis, acces physique) suffit a decoder le base64 et obtenir les credentials en clair.

### 1.4 — Deployment : l'API Gateway LogiStream

Fichier `tp3-app/k8s/api-gateway.yaml` :

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
  namespace: logistream
  labels:
    app: api-gateway
    team: backend
spec:
  replicas: 2
  selector:
    matchLabels:
      app: api-gateway
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: api-gateway
    spec:
      containers:
      - name: api-gateway
        image: europe-west9-docker.pkg.dev/ynov-cloud-tyson/tp2-registry/tp2-app:v1
        ports:
        - containerPort: 8080
        envFrom:
        - configMapRef:
            name: logistream-config
        env:
        - name: JWT_SECRET
          valueFrom:
            secretKeyRef:
              name: logistream-secrets
              key: JWT_SECRET
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "512Mi"
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 20
---
apiVersion: v1
kind: Service
metadata:
  name: api-gateway-svc
  namespace: logistream
spec:
  selector:
    app: api-gateway
  ports:
  - port: 80
    targetPort: 8080
  type: LoadBalancer
```

### 1.5 — Deployment : le Tracker Service

Fichier `tp3-app/k8s/tracker-service.yaml` :

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-service
  namespace: logistream
  labels:
    app: tracker-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: tracker-service
  template:
    metadata:
      labels:
        app: tracker-service
    spec:
      containers:
      - name: tracker-service
        image: europe-west9-docker.pkg.dev/ynov-cloud-tyson/tp2-registry/tp2-app:v1
        ports:
        - containerPort: 8080
        envFrom:
        - configMapRef:
            name: logistream-config
        env:
        - name: DB_URL
          valueFrom:
            secretKeyRef:
              name: logistream-secrets
              key: DB_URL
        - name: SERVICE_ROLE
          value: "tracker"
        resources:
          requests:
            cpu: "200m"
            memory: "256Mi"
          limits:
            cpu: "1000m"
            memory: "1Gi"
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 15
---
apiVersion: v1
kind: Service
metadata:
  name: tracker-service-svc
  namespace: logistream
spec:
  selector:
    app: tracker-service
  ports:
  - port: 80
    targetPort: 8080
  type: ClusterIP
```

### 1.6 — HorizontalPodAutoscaler : absorber les pics de charge

> LogiStream doit tenir les pics de 08h00 le lundi (tous les camions demarrent en meme temps). Sans HPA, les pods saturent. Avec HPA, Kubernetes ajoute automatiquement des replicas quand le CPU depasse le seuil.

Fichier `tp3-app/k8s/hpa.yaml` :

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: tracker-hpa
  namespace: logistream
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: tracker-service
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 60
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 70
```

### 1.7 — Appliquer et tester

```bash
# Appliquer tous les manifests
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/api-gateway.yaml
kubectl apply -f k8s/tracker-service.yaml
kubectl apply -f k8s/hpa.yaml
```

```
configmap/logistream-config created
secret/logistream-secrets created
deployment.apps/api-gateway created
service/api-gateway-svc created
deployment.apps/tracker-service created
service/tracker-service-svc created
horizontalpodautoscaler.autoscaling/tracker-hpa created
```

```bash
# Verifier que tous les pods sont Running (READY 1/1)
kubectl get pods -n logistream -w
```

```
NAME                               READY   STATUS    RESTARTS   AGE
api-gateway-5696857784-f7brt       1/1     Running   0          75s
api-gateway-5696857784-kxtpc       1/1     Running   0          59s
tracker-service-5655b9d8f9-66nvk   1/1     Running   0          55s
tracker-service-5655b9d8f9-nlsdj   1/1     Running   0          75s
```

```bash
# Recuperer l'IP publique de l'API Gateway
kubectl get service api-gateway-svc -n logistream

GATEWAY_IP=$(kubectl get service api-gateway-svc -n logistream \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Tester l'application
curl http://${GATEWAY_IP}/
curl http://${GATEWAY_IP}/health
```

```
NAME              TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)        AGE
api-gateway-svc   LoadBalancer   34.118.229.189   34.163.54.204   80:32671/TCP   10m

$ curl curl http://${GATEWAY_IP}/
{"message":"Hello from YNOV Cloud TP2","version":"2.1.0"}

$ curl http://${GATEWAY_IP}/health
{"status":"error","database":"disconnected"}
```

```bash
# Simuler une charge CPU pour observer le HPA en action
kubectl run load-test \
  --image=busybox:latest \
  --restart=Never \
  -n logistream \
  -- /bin/sh -c "while true; do wget -qO- http://tracker-service-svc/; done"

# Observer le HPA scaler (attendre 1-2 minutes)
kubectl get hpa tracker-hpa -n logistream -w
```

```
NAME          REFERENCE                    TARGETS                        MINPODS   MAXPODS   REPLICAS   AGE
tracker-hpa   Deployment/tracker-service   cpu: 48%/60%, memory: 13%/70%   2         10        2          13m
```

```bash
# Arreter le test de charge
kubectl delete pod load-test -n logistream
```

> **Question :** Le tracker-service recoit 80 evenements GPS/seconde en pointe. Le HPA est configure avec `minReplicas: 2` et `maxReplicas: 10`. Si chaque pod peut traiter 20 evenements/seconde a 60% CPU, combien de replicas le HPA maintiendra-t-il en regime de pointe ?
> **Reponse :** Chaque pod traite 20 evt/s a 60% CPU. Pour absorber 80 evt/s : 80 / 20 = 4 replicas. Le HPA se stabilise donc a 4 replicas, chaque pod etant exactement a son seuil de 60% CPU.

---

## Partie 2 — Apache Kafka sur Kubernetes avec Strimzi

> Strimzi est l'operateur Kubernetes officiel pour Apache Kafka. Il gere le cycle de vie complet du cluster Kafka (deploiement, configuration, mise a jour, scaling) via des Custom Resource Definitions (CRD) Kubernetes.

### 2.1 — Installer l'operateur Strimzi

```bash
# Creer le namespace pour Kafka
kubectl create namespace kafka

# Installer Strimzi via le fichier d'installation officiel
kubectl apply -f https://strimzi.io/install/latest?namespace=kafka \
  -n kafka
```

```
namespace/kafka created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-leader-election created
deployment.apps/strimzi-cluster-operator created
customresourcedefinition.apiextensions.k8s.io/kafkanodepools.kafka.strimzi.io created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-global created
configmap/strimzi-cluster-operator created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-namespaced created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-watched created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-kafka-broker-delegation created
customresourcedefinition.apiextensions.k8s.io/kafkaconnectors.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkabridges.kafka.strimzi.io created
clusterrole.rbac.authorization.k8s.io/strimzi-kafka-broker created
customresourcedefinition.apiextensions.k8s.io/kafkarebalances.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkatopics.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkausers.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkaconnects.kafka.strimzi.io created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-watched created
serviceaccount/strimzi-cluster-operator created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-entity-operator-delegation created
clusterrole.rbac.authorization.k8s.io/strimzi-kafka-client created
customresourcedefinition.apiextensions.k8s.io/strimzipodsets.core.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkamirrormaker2s.kafka.strimzi.io created
clusterrole.rbac.authorization.k8s.io/strimzi-entity-operator created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-kafka-client-delegation created
customresourcedefinition.apiextensions.k8s.io/kafkas.kafka.strimzi.io created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-leader-election created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator created...
```

```bash
# Verifier que l'operateur Strimzi est Running
kubectl get pods -n kafka
```

```
NAME                                        READY   STATUS    RESTARTS   AGE
strimzi-cluster-operator-659cd8ffc5-sk2n7   1/1     Running   0          85s
```

```bash
# Lister les Custom Resource Definitions (CRD) installees par Strimzi
kubectl get crds | grep kafka
```

```
kafkabridges.kafka.strimzi.io                          2026-04-28T10:39:15Z
kafkaconnectors.kafka.strimzi.io                       2026-04-28T10:39:15Z
kafkaconnects.kafka.strimzi.io                         2026-04-28T10:39:16Z
kafkamirrormaker2s.kafka.strimzi.io                    2026-04-28T10:39:18Z
kafkanodepools.kafka.strimzi.io                        2026-04-28T10:39:14Z
kafkarebalances.kafka.strimzi.io                       2026-04-28T10:39:16Z
kafkas.kafka.strimzi.io                                2026-04-28T10:39:19Z
kafkatopics.kafka.strimzi.io                           2026-04-28T10:39:16Z
kafkausers.kafka.strimzi.io                            2026-04-28T10:39:16Z
```

### 2.2 — Creer le cluster Kafka (KRaft mode — sans ZooKeeper)

> Le mode **KRaft** (Kafka Raft) est le mode natif de Kafka depuis la version 3.3 : il n'a plus besoin de ZooKeeper, ce qui simplifie le deploiement et reduit les couts.

Fichier `tp3-app/kafka/cluster.yaml` :

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: logistream-kafka
spec:
  replicas: 3
  roles:
    - controller
    - broker
  storage:
    type: persistent-claim
    size: 20Gi
    deleteClaim: true
  resources:
    requests:
      memory: "1Gi"
      cpu: "500m"
    limits:
      memory: "2Gi"
      cpu: "1"
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: logistream-kafka
  namespace: kafka
  annotations:
    strimzi.io/kraft: "enabled"
    strimzi.io/node-pools: "enabled"
spec:
  kafka:
    version: 4.2.0
    listeners:
    - name: plain
      port: 9092
      type: internal
      tls: false
    - name: external
      port: 9094
      type: loadbalancer
      tls: false
    config:
      num.partitions: 3
      default.replication.factor: 3
      min.insync.replicas: 2
      log.retention.hours: 168
      message.max.bytes: 10240
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

> **Note :** Le mode KRaft (sans ZooKeeper) est configure via l'annotation `strimzi.io/kraft: "enabled"` et un `KafkaNodePool` avec les roles `controller` + `broker`. Le champ `zookeeper.replicas: 0` du PDF n'est plus necessaire avec la derniere version de Strimzi qui utilise l'API v1.

```bash
# Deployer le cluster Kafka
kubectl apply -f kafka/cluster.yaml

# Suivre la progression (le cluster prend 3-5 minutes a demarrer)
kubectl get kafka logistream-kafka -n kafka -w
```

```
kafkanodepool.kafka.strimzi.io/dual-role created
kafka.kafka.strimzi.io/logistream-kafka created

NAME               READY   WARNINGS   KAFKA VERSION   METADATA VERSION
logistream-kafka   True               4.2.0           4.2-IV1
```

```bash
# Verifier les pods Kafka (3 brokers)
kubectl get pods -n kafka -l strimzi.io/cluster=logistream-kafka
```

```
NAME                                                READY   STATUS    RESTARTS   AGE
logistream-kafka-dual-role-0                        1/1     Running   0          2m47s
logistream-kafka-dual-role-1                        1/1     Running   0          2m47s
logistream-kafka-dual-role-2                        1/1     Running   0          2m47s
logistream-kafka-entity-operator-647db68f77-swsfl   2/2     Running   0          72s
```

### 2.3 — Creer les topics Kafka via KafkaTopic CRD

> En mode GitOps/Kubernetes, on ne cree pas les topics avec `kafka-topics.sh` en ligne de commande. On utilise la CRD `KafkaTopic` : l'operateur Strimzi cree et maintient le topic automatiquement.

Fichier `tp3-app/kafka/topics.yaml` :

```yaml
# Topic 1 : Positions GPS des camions (flux principal)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: truck-positions
  namespace: logistream
  labels:
    strimzi.io/cluster: logistream-kafka
spec:
  partitions: 6
  replicas: 3
  config:
    retention.ms: "86400000"
    cleanup.policy: "delete"
---
# Topic 2 : Alertes de livraison (retards, incidents)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: delivery-alerts
  namespace: logistream
  labels:
    strimzi.io/cluster: logistream-kafka
spec:
  partitions: 3
  replicas: 3
  config:
    retention.ms: "604800000"
    cleanup.policy: "delete"
---
# Topic 3 : Evenements de livraison (chargement, dechargement, signature)
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: delivery-events
  namespace: logistream
  labels:
    strimzi.io/cluster: logistream-kafka
spec:
  partitions: 3
  replicas: 3
  config:
    retention.ms: "2592000000"
    cleanup.policy: "delete"
```

```bash
# Appliquer les topics
kubectl apply -f kafka/topics.yaml

# Verifier la creation des topics
kubectl get kafkatopics -n kafka
```

```
kafkatopic.kafka.strimzi.io/truck-positions created
kafkatopic.kafka.strimzi.io/delivery-alerts created
kafkatopic.kafka.strimzi.io/delivery-events created

NAME              CLUSTER            PARTITIONS   REPLICATION FACTOR   READY
delivery-alerts   logistream-kafka   3            3                    True
delivery-events   logistream-kafka   3            3                    True
truck-positions   logistream-kafka   6            3                    True
```

```bash
# Verifier via l'outil CLI Kafka (depuis un pod temporaire)
kubectl run kafka-cli \
  --image=quay.io/strimzi/kafka:latest-kafka-4.2.0 \
  --restart=Never \
  -n kafka \
  -- /bin/bash -c "bin/kafka-topics.sh \
    --bootstrap-server logistream-kafka-kafka-bootstrap:9092 \
    --list"

kubectl logs kafka-cli -n kafka
kubectl delete pod kafka-cli -n kafka
```

```
delivery-alerts
delivery-events
truck-positions
```

### 2.4 — Producer Kafka : simuler les positions GPS des camions

Fichier `tp3-app/producer/gps-producer.js` :

```javascript
const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'logistream-gps-producer',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8,
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const TRUCKS = [
  { id: 'TRK-001', driver: 'Martin Dupont', route: 'Paris-Lyon' },
  { id: 'TRK-002', driver: 'Sophie Laurent', route: 'Lyon-Marseille' },
  { id: 'TRK-003', driver: 'Jean Moreau', route: 'Bordeaux-Paris' },
];

function generateGPSPosition(truck) {
  const basePositions = {
    'Paris-Lyon':       { lat: 48.8566, lng: 2.3522 },
    'Lyon-Marseille':   { lat: 45.7640, lng: 4.8357 },
    'Bordeaux-Paris':   { lat: 44.8378, lng: -0.5792 },
  };
  const base = basePositions[truck.route];
  return {
    truck_id: truck.id,
    driver: truck.driver,
    route: truck.route,
    latitude: base.lat + (Math.random() - 0.5) * 0.1,
    longitude: base.lng + (Math.random() - 0.5) * 0.1,
    speed_kmh: Math.floor(Math.random() * 40) + 70,
    fuel_level: Math.floor(Math.random() * 60) + 20,
    timestamp: new Date().toISOString(),
    event_type: 'GPS_UPDATE',
  };
}

async function startProducing() {
  await producer.connect();
  console.log('Producer Kafka connecte — debut de l\'envoi des positions GPS');

  let messageCount = 0;

  setInterval(async () => {
    const messages = TRUCKS.map(truck => {
      const position = generateGPSPosition(truck);
      return {
        key: truck.id,
        value: JSON.stringify(position),
        headers: {
          'event-type': 'gps-position',
          'source': 'mobile-app',
        },
      };
    });

    try {
      await producer.send({
        topic: 'truck-positions',
        messages: messages,
      });
      messageCount += messages.length;
      console.log(`[${new Date().toISOString()}] ${messages.length} positions envoyees (total: ${messageCount})`);
    } catch (err) {
      console.error('Erreur d\'envoi Kafka :', err.message);
    }
  }, 10000);

  setInterval(async () => {
    const truck = TRUCKS[Math.floor(Math.random() * TRUCKS.length)];
    const alert = {
      truck_id: truck.id,
      alert_type: 'DELIVERY_DELAY',
      severity: 'WARNING',
      message: `Camion ${truck.id} en retard de 15 minutes sur la route ${truck.route}`,
      estimated_delay_minutes: Math.floor(Math.random() * 30) + 5,
      timestamp: new Date().toISOString(),
    };
    await producer.send({
      topic: 'delivery-alerts',
      messages: [{ key: truck.id, value: JSON.stringify(alert) }],
    });
    console.log(`[ALERTE] ${alert.message}`);
  }, 30000);
}

process.on('SIGTERM', async () => {
  console.log('SIGTERM recu — arret propre du producer');
  await producer.disconnect();
  process.exit(0);
});

startProducing().catch(err => {
  console.error('Erreur fatale :', err);
  process.exit(1);
});
```

Fichier `tp3-app/producer/package.json` :

```json
{
  "name": "logistream-gps-producer",
  "version": "1.0.0",
  "description": "Simulateur GPS pour la flotte LogiStream",
  "main": "gps-producer.js",
  "scripts": {
    "start": "node gps-producer.js"
  },
  "dependencies": {
    "kafkajs": "^2.2.4"
  },
  "engines": { "node": "20" }
}
```

### 2.5 — Consumer Kafka : le Tracker Service traite les positions

Fichier `tp3-app/consumer/tracker-consumer.js` :

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'logistream-tracker-consumer',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'tracker-service-group',
});

const truckPositions = new Map();

async function startConsuming() {
  await consumer.connect();
  console.log('Consumer Kafka connecte');

  await consumer.subscribe({
    topics: ['truck-positions', 'delivery-alerts'],
    fromBeginning: false,
  });

  await consumer.run({
    partitionsConsumedConcurrently: 3,
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const value = JSON.parse(message.value.toString());
      const offset = message.offset;

      if (topic === 'truck-positions') {
        truckPositions.set(key, {
          ...value,
          last_seen: new Date().toISOString(),
          processed_at: Date.now(),
        });
        console.log(`[POSITION] ${value.truck_id} | ${value.latitude.toFixed(4)},${value.longitude.toFixed(4)} | ${value.speed_kmh} km/h | Partition ${partition} | Offset ${offset}`);

        if (value.speed_kmh < 5) {
          await publishAlert(value.truck_id, 'TRUCK_STOPPED', value);
        }
        if (value.fuel_level < 20) {
          await publishAlert(value.truck_id, 'LOW_FUEL', value);
        }
      } else if (topic === 'delivery-alerts') {
        console.log(`[ALERTE] ${value.alert_type} | ${value.truck_id} | ${value.message}`);
        await notifyDispatcher(value);
      }
    },
  });
}

async function publishAlert(truckId, alertType, context) {
  console.log(`[DETECTION] ${alertType} pour ${truckId}`);
}

async function notifyDispatcher(alert) {
  console.log(`[NOTIFICATION] Dispatcher alerte : ${alert.alert_type}`);
}

process.on('SIGTERM', async () => {
  console.log('SIGTERM recu — commit des offsets en cours');
  await consumer.disconnect();
  process.exit(0);
});

startConsuming().catch(err => {
  console.error('Erreur consumer :', err);
  process.exit(1);
});
```

> **Question :** Dans KafkaJS, le parametre `groupId` permet a plusieurs instances du `tracker-service` de former un **Consumer Group**. Expliquez comment Kafka distribue les partitions du topic `truck-positions` (6 partitions) entre 3 instances du consumer. Que se passe-t-il si on ajoute une 4e instance ?
>
> **Reponse :** Kafka distribue les partitions equitablement entre les membres d'un Consumer Group via un mecanisme de **rebalancing**. Avec 6 partitions et 3 instances, chaque instance recoit **2 partitions** (6 / 3 = 2). L'instance 1 traite les partitions 0 et 1, l'instance 2 les partitions 2 et 3, et l'instance 3 les partitions 4 et 5. L'ordre est garanti au sein de chaque partition (les messages d'un meme camion, ayant la meme cle `truck_id`, vont toujours dans la meme partition).
>
> Si on ajoute une **4e instance**, un rebalancing est declenche : Kafka redistribue les 6 partitions entre 4 consumers. Certains consumers auront 2 partitions, d'autres 1 seule (6 / 4 = 1 reste 2). Au-dela de 6 instances (= nombre de partitions), les instances supplementaires restent **inactives** (idle) car il n'y a pas assez de partitions a distribuer. C'est pourquoi le nombre de partitions doit etre >= au nombre max de consumers souhaite.

### 2.6 — Deployer le Producer et Consumer sur GKE

Fichier `tp3-app/producer/Dockerfile` :

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY ../../../Téléchargements .
USER node
CMD ["node", "gps-producer.js"]
```

Fichier `tp3-app/consumer/Dockerfile` :

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
USER node
CMD ["node", "tracker-consumer.js"]
```

```bash
PROJECT_ID=$(gcloud config get-value project)

# Builder et pusher les images
docker build -t europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/gps-producer:v1 ./tp3-app/producer/
docker push europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/gps-producer:v1

docker build -t europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tracker-consumer:v1 ./tp3-app/consumer/
docker push europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tracker-consumer:v1
```

```
gps-producer:v1: digest: sha256:018bb929bef43b3e619eb84cae1dff4199de3537f227bcbbc8178760d3e22928 size: 1991
tracker-consumer:v1: digest: sha256:de521ce9f42435eb8b5bafe03afa1c70b897b7e0081f1e718ca0a50ede1b143c size: 1991
```

Fichier `tp3-app/k8s/kafka-apps.yaml` :

```yaml
# Deployment du GPS Producer (simule la flotte de camions)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gps-producer
  namespace: logistream
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gps-producer
  template:
    metadata:
      labels:
        app: gps-producer
    spec:
      containers:
      - name: gps-producer
        image: europe-west9-docker.pkg.dev/ynov-cloud-tyson/tp2-registry/gps-producer:v1
        envFrom:
        - configMapRef:
            name: logistream-config
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
---
# Deployment du Tracker Consumer (traite les positions GPS)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tracker-consumer
  namespace: logistream
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tracker-consumer
  template:
    metadata:
      labels:
        app: tracker-consumer
    spec:
      containers:
      - name: tracker-consumer
        image: europe-west9-docker.pkg.dev/ynov-cloud-tyson/tp2-registry/tracker-consumer:v1
        envFrom:
        - configMapRef:
            name: logistream-config
        resources:
          requests:
            cpu: "200m"
            memory: "256Mi"
```

```bash
kubectl apply -f tp3-app/k8s/kafka-apps.yaml

# Observer les logs du producer (positions GPS envoyees)
kubectl logs deployment/gps-producer -n logistream --tail=5

# Observer les logs du consumer (positions GPS traitees)
kubectl logs deployment/tracker-consumer -n logistream --tail=10
```

```
# --- Logs Producer ---
Producer Kafka connecte — debut de l'envoi des positions GPS
[2026-04-07T10:31:24.786Z] 3 positions envoyees (total: 3)
[2026-04-07T10:31:34.588Z] 3 positions envoyees (total: 6)
[ALERTE] Camion TRK-001 en retard de 15 minutes sur la route Paris-Lyon
[2026-04-07T10:31:44.641Z] 3 positions envoyees (total: 9)

# --- Logs Consumer ---
Consumer Kafka connecte
[POSITION] TRK-001 | 48.8570,2.3589 | 82 km/h | Partition 2 | Offset 0
[POSITION] TRK-001 | 48.8886,2.3561 | 102 km/h | Partition 2 | Offset 1
[POSITION] TRK-001 | 48.8573,2.3032 | 73 km/h | Partition 2 | Offset 2
[ALERTE] DELIVERY_DELAY | TRK-001 | Camion TRK-001 en retard de 15 minutes sur la route Paris-Lyon
[NOTIFICATION] Dispatcher alerte : DELIVERY_DELAY
[POSITION] TRK-001 | 48.8425,2.3634 | 100 km/h | Partition 2 | Offset 3
```

```bash
# Verifier les offsets consommes (lag = retard du consumer)
kubectl run kafka-consumer-groups \
  --image=quay.io/strimzi/kafka:latest-kafka-4.2.0 \
  --restart=Never \
  -n kafka \
  -- /bin/bash -c "bin/kafka-consumer-groups.sh \
    --bootstrap-server logistream-kafka-kafka-bootstrap:9092 \
    --describe \
    --group tracker-service-group"

kubectl logs kafka-consumer-groups -n kafka
kubectl delete pod kafka-consumer-groups -n kafka
```

```
GROUP                 TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID                       HOST         CLIENT-ID
tracker-service-group delivery-alerts 0          -               0               -     logistream-tracker-consumer-...    /10.8.0.26   logistream-tracker-consumer
tracker-service-group truck-positions 1          7               7               0     logistream-tracker-consumer-...    /10.8.0.26   logistream-tracker-consumer
tracker-service-group truck-positions 4          -               0               -     logistream-tracker-consumer-...    /10.8.0.26   logistream-tracker-consumer
tracker-service-group delivery-alerts 1          -               0               -     logistream-tracker-consumer-...    /10.8.1.6    logistream-tracker-consumer
tracker-service-group truck-positions 3          -               0               -     logistream-tracker-consumer-...    /10.8.1.6    logistream-tracker-consumer
tracker-service-group truck-positions 5          7               7               0     logistream-tracker-consumer-...    /10.8.1.6    logistream-tracker-consumer
tracker-service-group delivery-alerts 2          2               2               0     logistream-tracker-consumer-...    /10.8.0.27   logistream-tracker-consumer
tracker-service-group truck-positions 0          -               0               -     logistream-tracker-consumer-...    /10.8.0.27   logistream-tracker-consumer
tracker-service-group truck-positions 2          7               7               0     logistream-tracker-consumer-...    /10.8.0.27   logistream-tracker-consumer

# LAG = 0 sur toutes les partitions actives -> les consumers traitent en temps reel
# 3 consumers se repartissent 6 partitions truck-positions (2 chacun) + 3 partitions delivery-alerts (1 chacun)
```

---

## Partie 3 — Pipeline CI/CD avec GitLab CI

> LogiStream veut automatiser le deploiement : chaque push sur main doit declencher les tests, builder les nouvelles images et deployer automatiquement sur GKE, sans intervention manuelle.

### 3.1 — Preparer le repository

```bash
git init
git add .
git commit -m "feat: initial commit LogiStream GKE + Kafka"
git remote add origin <URL_GITLAB>
git push -u origin main
```

### 3.2 — Secrets CI/CD requis

Dans **Settings > CI/CD > Variables**, creer :

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | ID du projet GCP (`ynov-cloud-tyson`) |
| `GCP_SA_KEY` | Cle JSON du Service Account CI/CD |
| `GKE_CLUSTER` | `logistream-cluster` |
| `GKE_REGION` | `europe-west9` |

```bash
PROJECT_ID=$(gcloud config get-value project)

# Creer le Service Account pour CI/CD
gcloud iam service-accounts create logistream-cicd-sa \
  --display-name="LogiStream CI/CD"

SA_EMAIL="logistream-cicd-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Permissions necessaires
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/container.developer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/container.clusterViewer"

# Generer la cle JSON
gcloud iam service-accounts keys create cicd-key.json --iam-account=${SA_EMAIL}
cat cicd-key.json  # Copier dans la variable CI/CD GCP_SA_KEY
rm cicd-key.json   # Supprimer immediatement apres copie
```

```
Created service account [logistream-cicd-sa].
Updated IAM policy for project [ynov-cloud-tyson].
created key [...] of type [json] as [cicd-key.json]
```

### 3.3 — Pipeline CI/CD complet

Fichier `.gitlab-ci.yml` :

```yaml
stages:
  - test
  - build
  - deploy

variables:
  REGISTRY: europe-west9-docker.pkg.dev

# ==========================================================
# Job 1 : Tests (s'execute sur chaque MR et chaque push)
# ==========================================================
test:
  stage: test
  image: node:20
  script:
    - cd tp3/producer && npm ci && cd ../..
    - cd tp3/consumer && npm ci && cd ../..
    - echo "Dependances installees — tests OK"
    - |
      curl -sL https://github.com/instrumenta/kubeval/releases/latest/download/kubeval-linux-amd64.tar.gz | tar xz
      ./kubeval tp3-app/k8s/*.yaml --ignore-missing-schemas || echo "Validation terminee"

# ==========================================================
# Job 2 : Build & Push (uniquement sur push main)
# ==========================================================
build-push:
  stage: build
  image: docker:latest
  services:
    - docker:dind
  only:
    - main
  before_script:
    - echo "$GCP_SA_KEY" | docker login -u _json_key --password-stdin https://${REGISTRY}
  script:
    - export SHA=${CI_COMMIT_SHA}
    - docker build -t ${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/gps-producer:${SHA} ./tp3-app/producer/
    - docker push ${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/gps-producer:${SHA}
    - docker build -t ${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/tracker-consumer:${SHA} ./tp3-app/consumer/
    - docker push ${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/tracker-consumer:${SHA}
    - echo "PRODUCER_TAG=${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/gps-producer:${SHA}" >> build.env
    - echo "CONSUMER_TAG=${REGISTRY}/${GCP_PROJECT_ID}/tp2-registry/tracker-consumer:${SHA}" >> build.env
  artifacts:
    reports:
      dotenv: build.env

# ==========================================================
# Job 3 : Deploy sur GKE
# ==========================================================
deploy:
  stage: deploy
  image: google/cloud-sdk:latest
  only:
    - main
  dependencies:
    - build-push
  script:
    - echo "$GCP_SA_KEY" | gcloud auth activate-service-account --key-file=-
    - gcloud container clusters get-credentials ${GKE_CLUSTER} --region=${GKE_REGION} --project=${GCP_PROJECT_ID}
    - kubectl set image deployment/gps-producer gps-producer=${PRODUCER_TAG} -n logistream
    - kubectl set image deployment/tracker-consumer tracker-consumer=${CONSUMER_TAG} -n logistream
    - kubectl rollout status deployment/gps-producer -n logistream --timeout=5m
    - kubectl rollout status deployment/tracker-consumer -n logistream --timeout=5m
    - kubectl get pods -n logistream
    - kubectl get kafkatopics -n kafka
    - echo "Deploiement LogiStream reussi"
```

```bash
# Declencher le pipeline
git add .
git commit -m "feat: add CI/CD pipeline GitLab"
git push origin main
```

```
# Pipeline GitLab CI — 3 jobs passes :
# - test : npm ci (producer + consumer) + kubeval manifests K8s -> OK
# - build-push : docker build + push des 2 images vers Artifact Registry -> OK
# - deploy : kubectl set image + rollout status sur GKE -> OK
```

> **Question :** Le pipeline valide les manifests Kubernetes avec `kubeval`. Quel autre outil de validation vous permettrait de verifier que les manifests respectent les regles de securite de l'entreprise (ex: tout container doit avoir des `resources.limits`, tout pod doit avoir une `readinessProbe`) ?
>
> **Reponse :** L'outil **OPA/Gatekeeper** (Open Policy Agent) ou **Kyverno** permettrait de definir et appliquer des politiques de securite personnalisees sur les manifests Kubernetes. Par exemple, on peut creer une politique Kyverno `require-resource-limits` qui refuse tout Deployment sans `resources.limits` et une politique `require-readiness-probe` qui exige une `readinessProbe`. Ces outils s'integrent dans le pipeline CI/CD (validation statique avec `kyverno apply`) ET dans le cluster (admission controller qui bloque les deployments non conformes). Un autre outil populaire est **Datree** ou **Kubeconform** pour la validation statique, et **Polaris** de Fairwinds pour l'audit de securite des manifests K8s.

---

## Partie 4 — Observabilite du cluster Kafka et des microservices

### 4.1 — Metriques Kafka avec Cloud Monitoring

```bash
# Observer les metriques des pods Kafka directement
kubectl top pods -n kafka -l strimzi.io/cluster=logistream-kafka
```

```
NAME                                                CPU(cores)   MEMORY(bytes)
logistream-kafka-dual-role-0                        25m          576Mi
logistream-kafka-dual-role-1                        39m          659Mi
logistream-kafka-dual-role-2                        29m          569Mi
logistream-kafka-entity-operator-5f895dbb65-l2kcr   17m          412Mi
```

```bash
# Metriques des pods applicatifs
kubectl top pods -n logistream
```

```
NAME                                CPU(cores)   MEMORY(bytes)
api-gateway-6cb97f5bc7-h2gmf        1m           25Mi
api-gateway-6cb97f5bc7-zzz8j        1m           44Mi
gps-producer-55d9967c9-rvzcn        45m          18Mi
tracker-consumer-6b7bf48bcc-7p6sn   48m          25Mi
tracker-consumer-6b7bf48bcc-8jdsp   48m          21Mi
tracker-consumer-6b7bf48bcc-sv9kp   71m          23Mi
tracker-service-849df4458-bm97g     1m           23Mi
tracker-service-849df4458-mdtpq     1m           23Mi
```

```bash
# Verifier le lag du consumer group (retard dans la consommation)
kubectl run kafka-lag-check \
  --image=quay.io/strimzi/kafka:latest-kafka-4.2.0 \
  --restart=Never \
  -n kafka \
  -- /bin/bash -c "
    bin/kafka-consumer-groups.sh \
      --bootstrap-server logistream-kafka-kafka-bootstrap:9092 \
      --describe \
      --group tracker-service-group 2>/dev/null;
    echo '---';
    bin/kafka-topics.sh \
      --bootstrap-server logistream-kafka-kafka-bootstrap:9092 \
      --describe \
      --topic truck-positions
  "

sleep 30
kubectl logs kafka-lag-check -n kafka
kubectl delete pod kafka-lag-check -n kafka
```

```
GROUP                 TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                       HOST          CLIENT-ID
tracker-service-group delivery-alerts 2          134             134             0    logistream-tracker-consumer-...    /10.8.0.30    logistream-tracker-consumer
tracker-service-group truck-positions 0          -               0               -    logistream-tracker-consumer-...    /10.8.0.30    logistream-tracker-consumer
tracker-service-group truck-positions 2          628             628             0    logistream-tracker-consumer-...    /10.8.0.30    logistream-tracker-consumer
tracker-service-group delivery-alerts 0          -               0               -    logistream-tracker-consumer-...    /10.8.0.138   logistream-tracker-consumer
tracker-service-group truck-positions 1          628             628             0    logistream-tracker-consumer-...    /10.8.0.138   logistream-tracker-consumer
tracker-service-group truck-positions 4          -               0               -    logistream-tracker-consumer-...    /10.8.0.138   logistream-tracker-consumer
tracker-service-group delivery-alerts 1          74              74              0    logistream-tracker-consumer-...    /10.8.0.29    logistream-tracker-consumer
tracker-service-group truck-positions 3          -               0               -    logistream-tracker-consumer-...    /10.8.0.29    logistream-tracker-consumer
tracker-service-group truck-positions 5          628             628             0    logistream-tracker-consumer-...    /10.8.0.29    logistream-tracker-consumer
---
Topic: truck-positions  PartitionCount: 6  ReplicationFactor: 3
  Configs: min.insync.replicas=2, cleanup.policy=delete, retention.ms=86400000
  Partition: 0  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1
  Partition: 1  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
  Partition: 2  Leader: 1  Replicas: 1,2,0  Isr: 1,2,0
  Partition: 3  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
  Partition: 4  Leader: 1  Replicas: 1,2,0  Isr: 1,2,0
  Partition: 5  Leader: 2  Replicas: 2,0,1  Isr: 2,0,1

# LAG = 0 partout, ISR = 3/3 sur toutes les partitions -> cluster sain
```

### 4.2 — Logs structures et requetes Cloud Logging

```bash
# Logs du producer (positions GPS envoyees)
gcloud logging read \
  'resource.type="k8s_container"
  AND resource.labels.namespace_name="logistream"
  AND resource.labels.container_name="gps-producer"' \
  --limit=5 \
  --format="table(timestamp,textPayload)"
```

```
TIMESTAMP                       TEXT_PAYLOAD
2026-04-07T12:16:01.999126195Z  [2026-04-07T12:16:01.998Z] 3 positions envoyees (total: 141)
2026-04-07T12:15:52.045234289Z  [2026-04-07T12:15:52.045Z] 3 positions envoyees (total: 138)
2026-04-07T12:15:41.998320323Z  [2026-04-07T12:15:41.998Z] 3 positions envoyees (total: 135)
2026-04-07T12:15:41.989982944Z  [ALERTE] Camion TRK-002 en retard de 15 minutes sur la route Lyon-Marseille
2026-04-07T12:15:31.999158869Z  [2026-04-07T12:15:31.998Z] 3 positions envoyees (total: 132)
```

```bash
# Logs du consumer filtre sur les alertes seulement
gcloud logging read \
  'resource.type="k8s_container"
  AND resource.labels.namespace_name="logistream"
  AND resource.labels.container_name="tracker-consumer"
  AND textPayload:"[ALERTE]"' \
  --limit=5 \
  --format="table(timestamp,textPayload)"
```

```
TIMESTAMP                       TEXT_PAYLOAD
2026-04-07T12:16:11.987881631Z  [ALERTE] DELIVERY_DELAY | TRK-002 | Camion TRK-002 en retard de 15 minutes sur la route Lyon-Marseille
2026-04-07T12:15:41.989072445Z  [ALERTE] DELIVERY_DELAY | TRK-002 | Camion TRK-002 en retard de 15 minutes sur la route Lyon-Marseille
2026-04-07T12:15:11.988788544Z  [ALERTE] DELIVERY_DELAY | TRK-003 | Camion TRK-003 en retard de 15 minutes sur la route Bordeaux-Paris
2026-04-07T12:14:41.987032575Z  [ALERTE] DELIVERY_DELAY | TRK-002 | Camion TRK-002 en retard de 15 minutes sur la route Lyon-Marseille
2026-04-07T12:14:11.987643010Z  [ALERTE] DELIVERY_DELAY | TRK-003 | Camion TRK-003 en retard de 15 minutes sur la route Bordeaux-Paris
```

```bash
# Logs d'erreur du cluster Kafka
gcloud logging read \
  'resource.type="k8s_container"
  AND resource.labels.namespace_name="kafka"
  AND resource.labels.container_name:"kafka"
  AND severity>=ERROR' \
  --limit=5
```

```
# Aucune erreur Kafka relevee — cluster sain
```

### 4.3 — Creer une alerte sur le Consumer Lag

> Un consumer lag eleve sur `truck-positions` signifie que le Tracker Service n'arrive pas a traiter les positions assez vite : les chauffeurs verront des positions en retard dans l'interface.

Dans la console GCP (**Monitoring > Alerting > Create Policy**) :

- **Etape 1 — Metrique :**
  - Type : Kubernetes Container
  - Metrique : `kubernetes.io/container/memory/request_utilization`
  - Filtre : `namespace_name = "logistream"`, `container_name = "tracker-consumer"`

- **Etape 2 — Condition :**
  - Seuil : > 80%
  - Duree : 5 minutes

- **Etape 3 — Notification :**
  - Canal : Email
  - Nom : "LogiStream Tracker Consumer — Memoire haute"

**Metriques observees au repos :**

| Metrique | Valeur observee |
|----------|----------------|
| CPU moyen tracker-consumer | ~55 m (soit ~5.5%) |
| RAM moyenne tracker-consumer | ~23 Mi |
| Pods HPA actifs au repos | 2 |

> **Question :** Vous observez que le consumer lag sur `truck-positions` monte progressivement au cours du temps. Vous avez 3 instances du tracker-consumer (3 replicas). Quelles sont les 3 actions a envisager dans l'ordre pour resoudre ce probleme ?
>
> **Reponse :**
> 1. **Augmenter le nombre de replicas du consumer** (scale horizontal) : passer de 3 a 6 replicas par exemple, pour que chaque instance ait moins de partitions a traiter (6 partitions / 6 instances = 1 partition par instance). C'est la solution la plus rapide et la moins risquee.
> 2. **Optimiser le code du consumer** : profiler le traitement (`eachMessage`) pour identifier les goulots d'etranglement (appels DB lents, serialisation, I/O bloquant). Augmenter `partitionsConsumedConcurrently` pour traiter plus de messages en parallele. Passer en mode `eachBatch` au lieu de `eachMessage` pour reduire l'overhead par message.
> 3. **Augmenter le nombre de partitions du topic** : si le nombre de consumers depasse deja le nombre de partitions, il faut augmenter les partitions (ex: passer de 6 a 12) pour permettre un parallelisme plus eleve. Attention : cette operation est irreversible et necessite de re-partitionner les donnees existantes.

---

## Nettoyage Final

```bash
# 1. Supprimer les ressources applicatives Kubernetes
kubectl delete -f tp3-app/k8s/ -n logistream
```

```
deployment.apps "api-gateway" deleted
service "api-gateway-svc" deleted
configmap "logistream-config" deleted
horizontalpodautoscaler.autoscaling "tracker-hpa" deleted
deployment.apps "gps-producer" deleted
deployment.apps "tracker-consumer" deleted
secret "logistream-secrets" deleted
deployment.apps "tracker-service" deleted
service "tracker-service-svc" deleted
```

```bash
# 2. Supprimer le cluster Kafka (et les volumes persistants)
kubectl delete kafka logistream-kafka -n kafka
kubectl delete kafkanodepool dual-role -n kafka
kubectl delete kafkatopics --all -n kafka
```

```
kafka.kafka.strimzi.io "logistream-kafka" deleted
kafkanodepool.kafka.strimzi.io "dual-role" deleted
kafkatopic.kafka.strimzi.io "delivery-alerts" deleted
kafkatopic.kafka.strimzi.io "delivery-events" deleted
kafkatopic.kafka.strimzi.io "truck-positions" deleted
```

```bash
# 3. Desinstaller Strimzi
kubectl delete -f https://strimzi.io/install/latest?namespace=kafka -n kafka

# 4. Supprimer les namespaces
kubectl delete namespace logistream kafka
```

```
namespace "logistream" deleted
namespace "kafka" deleted
```

```bash
# 5. Supprimer le cluster GKE (IRREVERSIBLE)
gcloud container clusters delete logistream-cluster --region=europe-west9 --quiet
```

```
Deleting cluster logistream-cluster...done.
Deleted [https://container.googleapis.com/v1/projects/ynov-cloud-tyson/zones/europe-west9/clusters/logistream-cluster].
```

```bash
# 6. Verification finale
gcloud container clusters list
```

```
# Aucun cluster restant — nettoyage complet
```

---

## Recapitulatif — Competences validees

- Kubernetes : Deployment, Service, ConfigMap, Secret, HPA
- Apache Kafka (Strimzi) : cluster KRaft, KafkaTopic CRD, Consumer Groups
- KafkaJS : Producer avec partitioning par cle, Consumer avec groupId
- CI/CD : GitLab CI (test > build > push > deploy sur GKE)
- Observabilite : Cloud Logging, Cloud Monitoring, consumer lag
