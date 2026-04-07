# TP 2 — Docker Avancé, Cloud Run & Networking GCP

**Cours 2 | Développer pour le Cloud | YNOV Campus Montpellier — Master 2**

**Date :** 20/03/2026 | **Durée TP :** 3h | **Plateforme :** Google Cloud Platform

---

## Livrables

| # | Livrable | Statut                                |
|---|---|---------------------------------------|
| 1 | URL publique Cloud Run : `https://tp2-service-390464221655.europe-west9.run.app` | ✅                                     |
| 2 | Capture `docker images` (réduction de taille standard vs multi-stage) | ✅ `screenshots/docker-images.png`     |
| 3 | Capture service Cloud Run actif dans la console GCP | ✅ `screenshots/cloud-run-service.png` |
| 4 | Fichier `docker-compose.yml` fonctionnel | ✅                                     |
| 5 | `README.md` expliquant l'architecture et les commandes | ✅ `tp2-app/README.md`                 |

---

## Partie 1 — Docker Multi-Stage Build (30 min)

*Le build multi-stage permet de séparer l'environnement de **compilation** de l'environnement de **production**, réduisant drastiquement la taille de l'image finale.*

### 1.1 — Comprendre le problème

**Dockerfile naïf (`tp2-app/Dockerfile.naive`) :**

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
CMD ["node", "dist/index.js"]
```

```bash
cd tp2-app
docker build -f Dockerfile.naive -t tp2-naive:v1 .
docker images tp2-naive:v1
# Taille : 202 MB
```

### 1.2 — Dockerfile Multi-Stage

**`tp2-app/Dockerfile` :**

```dockerfile
# ============================================
# Stage 1 : Build — Environnement de compilation
# ============================================
FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
COPY tsconfig.json ./

RUN npm install

COPY src/ ./src/

RUN npm run build

# ============================================
# Stage 2 : Runtime — Image de production minimale
# ============================================
FROM node:20-alpine AS runtime

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

EXPOSE 8080
ENV APP_ENV=production
ENV NODE_ENV=production

CMD ["node", "dist/index.js"]
```

```bash
docker build -t tp2-app:v1 .

docker images | grep tp2
# tp2-naive   v1   202MB
# tp2-app     v1   139MB

# Réduction de taille : (202 - 139) / 202 * 100 = 31.2 %
```

**Question :** Pourquoi les outils de build (TypeScript, gcc, etc.) ne doivent-ils pas être présents dans l'image de production ?

> **Réponse :** Les outils de build comme TypeScript, gcc ou npm ne servent qu'à transformer le code source en artefact exécutable. Une fois la compilation terminée, ils deviennent inutiles en production. Les laisser dans l'image finale pose trois problèmes :
>
> 1. **Taille** : ces outils sont lourds et augmentent inutilement l'image, ce qui ralentit les pulls et les déploiements (on le voit concrètement : 202 MB vs 139 MB).
> 2. **Sécurité** : chaque binaire supplémentaire est une surface d'attaque potentielle. Un attaquant qui prend le contrôle du conteneur peut exploiter ces outils pour compiler du code malveillant ou escalader ses privilèges.
> 3. **Principe du moindre privilège** : une image de production ne doit contenir que ce qui est strictement nécessaire à l'exécution — ici, Node.js et le code compilé dans `dist/`.

### 1.3 — .dockerignore

**`tp2-app/.dockerignore` :**

```
node_modules
dist
*.log
.env
.git
*.md
Dockerfile*
docker-compose*
```

---

## Partie 2 — Docker Compose : Stack App + PostgreSQL (30 min)

*Docker Compose orchestre plusieurs conteneurs en local. On simule ici un environnement de développement complet.*

### 2.1 — Ajouter la connexion base de données

**`tp2-app/src/index.ts`** modifié avec import `pg`, pool de connexion PostgreSQL, et routes `/health` (avec check DB) et `/db` (compteur de visites).

Ajout des dépendances :

```bash
npm install pg
npm install --save-dev @types/pg
```

### 2.2 — Écrire le fichier docker-compose.yml

**`tp2-app/docker-compose.yml` :**

```yaml
version: "3.9"

services:
  web:
    build: .
    ports:
      - "8080:8080"
    environment:
      - APP_ENV=development
      - DB_HOST=db
      - DB_PORT=5432
      - DB_NAME=ynov_db
      - DB_USER=ynov
      - DB_PASSWORD=secret_password
    depends_on:
      db:
        condition: service_healthy
    networks:
      - app-network

  db:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=ynov_db
      - POSTGRES_USER=ynov
      - POSTGRES_PASSWORD=secret_password
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ynov -d ynov_db"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - app-network

volumes:
  pgdata:

networks:
  app-network:
    driver: bridge
```

**Question :** Pourquoi utilise-t-on `condition: service_healthy` plutôt que `condition: service_started` pour `depends_on` ?

> **Réponse :** Un conteneur démarré n'est pas forcément prêt à recevoir des connexions. Avec `service_started`, Docker attend uniquement que le processus PostgreSQL soit lancé — mais PostgreSQL a besoin de quelques secondes supplémentaires pour initialiser ses fichiers et écouter sur le port 5432. Si l'application Node.js démarre trop tôt, elle tente de se connecter à une base qui n'est pas encore prête et échoue.
>
> `service_healthy` résout ce problème en attendant que le healthcheck (`pg_isready -U ynov -d ynov_db`) retourne un succès, ce qui garantit que PostgreSQL accepte réellement les connexions avant que le service `web` ne démarre.

### 2.3 — Lancer et tester la stack

```bash
# Démarrer tous les services en arrière-plan
docker compose up -d --build

# Vérifier l'état des services
docker compose ps

# Tester l'application
curl http://localhost:8080/
curl http://localhost:8080/health
curl http://localhost:8080/db
curl http://localhost:8080/db

# Voir les logs en temps réel
docker compose logs -f

# Arrêter sans supprimer les volumes (données conservées)
docker compose down

# Voir les logs en temps réel
docker compose logs -f

# Arrêter ET supprimer les volumes (reset complet)
docker compose down -v
```

---

## Partie 3 — Artifact Registry & Push de l'Image (20 min)

*Artifact Registry est le registry privé de GCP. Il remplace Container Registry (gcr.io) et supporte Docker, Maven, npm, Python, etc.*

### 3.1 — Créer un repository Artifact Registry

```bash
gcloud artifacts repositories create tp2-registry \
  --repository-format=docker \
  --location=europe-west9 \
  --description="Registry TP2 YNOV"

# Lister les repositories existants
gcloud artifacts repositories list
```

### 3.2 — Authentifier Docker avec Artifact Registry

```bash
# Configurer Docker pour utiliser gcloud comme credential helper
# europe-west9-docker.pkg.dev est l'endpoint Artifact Registry pour Paris
gcloud auth configure-docker europe-west9-docker.pkg.dev

cat ~/.docker/config.json | grep -A3 "credHelpers"
```

### 3.3 — Tagger et pousser l'image

```bash
PROJECT_ID=$(gcloud config get-value project)
IMAGE_TAG="europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tp2-app:v1"

echo "Image tag : ${IMAGE_TAG}"
# Image tag : europe-west9-docker.pkg.dev/ynov-cloud-tyson/tp2-registry/tp2-app:v1

# Tagger l'image locale
docker tag tp2-app:v1 ${IMAGE_TAG}

# Pousser l'image
docker push ${IMAGE_TAG}

# Vérifier que l'image est bien dans le registry
gcloud artifacts docker images list \
  europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry
```

---

## Partie 4 — Déploiement sur Cloud Run (20 min)

*Cloud Run est le service PaaS serverless de GCP pour les conteneurs. Il scale automatiquement de 0 à N instances selon le trafic, et vous ne payez qu'à l'usage.*

### 4.1 — Déployer le service

```bash
PROJECT_ID=$(gcloud config get-value project)
IMAGE="europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tp2-app:v1"

gcloud run deploy tp2-service \
  --image=${IMAGE} \
  --region=europe-west9 \
  --platform=managed \
  --allow-unauthenticated \
  --port=8080 \
  --memory=512Mi \
  --cpu=1 \
  --max-instances=3 \
  --set-env-vars="APP_ENV=production"

# URL retournée :
# https://tp2-service-390464221655.europe-west9.run.app
```

> **Note :** Cloud Run ne peut pas se connecter directement à votre PostgreSQL local. Le service `/db` retournera une erreur de connexion — ce n'est pas un problème. On utilisera Cloud SQL en cours 4.

### 4.2 — Tester le déploiement

```bash
SERVICE_URL=$(gcloud run services describe tp2-service \
  --region=europe-west9 \
  --format='value(status.url)')

echo "URL du service : ${SERVICE_URL}"
# URL du service : https://tp2-service-skfw2up62a-od.a.run.app

curl ${SERVICE_URL}/
# {"message":"Hello from YNOV Cloud TP2","version":"2.0.0","stage":"production"}

curl ${SERVICE_URL}/health
# {"status":"ok"}

gcloud run services describe tp2-service --region=europe-west9
```

**Question :** Quelle est la différence entre `--max-instances=3` et `--min-instances=1` dans Cloud Run ?

> **Réponse :** `--max-instances=3` : plafonne le nombre de conteneurs lancés en parallèle (maîtrise des coûts et de la charge).
>
> `--min-instances=1` : garde au moins 1 instance toujours active, ce qui élimine le cold start au prix d'un coût fixe permanent.
>
> Sans `--min-instances`, Cloud Run redescend à 0 après inactivité — économique, mais la première requête subit une latence de démarrage.

### 4.3 — Observer le comportement de scale à zéro

```bash
# Mesurer le temps de réponse
time curl ${SERVICE_URL}/health

# Cold start :
# real    0m0,735s
# user    0m0,039s
# sys     0m0,015s

# Warm start : 
# real    0m0,142s
# user    0m0,033s
# sys     0m0,019s 
```

---

## Partie 5 — Networking GCP : VPC & Firewall (20 min)

*GCP crée un VPC "default" automatiquement. Pour des déploiements professionnels, on crée son propre VPC avec des sous-réseaux isolés.*

### 5.1 — Créer un VPC personnalisé

```bash
# Créer un VPC en mode custom (pas de sous-réseaux automatiques)
gcloud compute networks create tp2-vpc \
  --subnet-mode=custom

# Créer un sous-réseau public (pour les services exposés à internet)
gcloud compute networks subnets create tp2-subnet-public \
  --network=tp2-vpc \
  --region=europe-west9 \
  --range=10.10.1.0/24

# Créer un sous-réseau privé (pour les bases de données, non exposé)
gcloud compute networks subnets create tp2-subnet-private \
  --network=tp2-vpc \
  --region=europe-west9 \
  --range=10.10.2.0/24
```

**Question :** Pourquoi sépare-t-on les ressources applicatives et les bases de données dans des sous-réseaux différents ?

> **Réponse :** Séparer les ressources en sous-réseaux distincts permet d'appliquer des règles de pare-feu adaptées à chaque zone : le sous-réseau public ouvre les ports HTTP/HTTPS vers internet, le sous-réseau privé n'accepte que le trafic interne venant de la couche applicative.
>
> L'intérêt sécuritaire est le principe de **défense en profondeur** : un attaquant qui compromet un serveur web se retrouve bloqué au niveau réseau avant d'atteindre la base de données, qui n'est jamais exposée directement à internet.

### 5.2 — Règles de pare-feu (Firewall Rules)

```bash
# Règle 1 : Autoriser HTTP (port 80) depuis internet
gcloud compute firewall-rules create tp2-allow-http \
  --network=tp2-vpc \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:80 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=http-server

# Règle 2 : Autoriser HTTPS (port 443)
gcloud compute firewall-rules create tp2-allow-https \
  --network=tp2-vpc \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:443 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=http-server

# Règle 3 : Autoriser PostgreSQL (port 5432) UNIQUEMENT depuis le sous-réseau applicatif
gcloud compute firewall-rules create tp2-allow-postgres \
  --network=tp2-vpc \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:5432 \
  --source-ranges=10.10.1.0/24 \
  --target-tags=db-server

# Lister les règles de firewall du VPC
gcloud compute firewall-rules list --filter="network=tp2-vpc"
```

**Question :** Quelle est la différence entre un Security Group (AWS) et une Firewall Rule (GCP) ?

> **Réponse :** Les deux sont stateful, mais leur modèle d'application diffère.
>
> Un **Security Group AWS** s'attache directement à une interface réseau (ENI) : les règles suivent l'instance. Toutes les règles sont additives et s'appliquent simultanément.
>
> Une **Firewall Rule GCP** s'applique au niveau du **VPC** et cible les instances via des **tags réseau**. Les règles fonctionnent par **priorité** (0–65535) : la première règle qui correspond l'emporte, les suivantes sont ignorées.

### 5.3 — Nettoyage du VPC

```bash
# Supprimer dans l'ordre inverse (les règles avant le VPC)
gcloud compute firewall-rules delete tp2-allow-http --quiet
gcloud compute firewall-rules delete tp2-allow-https --quiet
gcloud compute firewall-rules delete tp2-allow-postgres --quiet
gcloud compute networks subnets delete tp2-subnet-public --region=europe-west9 --quiet
gcloud compute networks subnets delete tp2-subnet-private --region=europe-west9 --quiet
gcloud compute networks delete tp2-vpc --quiet
```

---

## Partie 6 — Cloud Storage Avancé : Versioning & Lifecycle (20 min)

*En production, on ne supprime jamais accidentellement des fichiers. Le versioning et les règles de lifecycle protègent les données et optimisent les coûts.*

### 6.1 — Bucket avec versioning activé

```bash
PROJECT_ID=$(gcloud config get-value project)
BUCKET="ynov-tp2-versioned-${PROJECT_ID}"

# Créer un bucket avec contrôle d'accès unifié
gcloud storage buckets create gs://${BUCKET} \
  --location=europe-west9 \
  --uniform-bucket-level-access

# Activer le versioning
gcloud storage buckets update gs://${BUCKET} \
  --versioning

# Vérifier
gcloud storage buckets describe gs://${BUCKET}
# versioning_enabled: true
```

### 6.2 — Tester le versioning

```bash
# Créer et uploader 3 versions successives
echo "Version 1 - $(date)" > config.json
gcloud storage cp config.json gs://${BUCKET}/

echo "Version 2 - $(date)" > config.json
gcloud storage cp config.json gs://${BUCKET}/

echo "Version 3 - $(date)" > config.json
gcloud storage cp config.json gs://${BUCKET}/

# Lister TOUTES les versions
gcloud storage ls -a gs://${BUCKET}/config.json
# 3 versions avec des numéros de génération différents :
# gs://ynov-tp2-versioned-ynov-cloud-tyson/config.json#1775557196872653
# gs://ynov-tp2-versioned-ynov-cloud-tyson/config.json#1775557208138930
# gs://ynov-tp2-versioned-ynov-cloud-tyson/config.json#1775557216185409
```

### 6.3 — Règles de lifecycle automatisées

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": {
          "numNewerVersions": 3,
          "isLive": false
        }
      },
      {
        "action": {
          "type": "SetStorageClass",
          "storageClass": "NEARLINE"
        },
        "condition": {
          "age": 30,
          "isLive": true
        }
      }
    ]
  }
}
```

```bash
gcloud storage buckets update gs://${BUCKET} \
  --lifecycle-file=lifecycle.json

gcloud storage buckets describe gs://${BUCKET}
# lifecycle_config confirmée avec les 2 règles
```

**Question :** Expliquez les deux règles lifecycle. Quel est l'intérêt économique de passer en NEARLINE après 30 jours ?

> **Règle 1 :** Supprime automatiquement les anciennes versions non-live (archivées) lorsqu'il existe déjà 3 versions plus récentes. Cela évite l'accumulation infinie de versions et maîtrise les coûts de stockage.
>
> **Règle 2 :** Après 30 jours, les objets live (version courante) sont automatiquement déplacés de la classe STANDARD vers NEARLINE.
>
> **Intérêt économique :** STANDARD coûte ~$0.020/Go/mois, NEARLINE ~$0.010/Go/mois (2x moins cher). Pour des fichiers rarement consultés après 30 jours, cette transition réduit les coûts de stockage de 50% sans intervention manuelle. Le compromis : NEARLINE facture un coût à chaque lecture, donc elle n'est rentable que si les fichiers sont peu accédés.

### 6.4 — Nettoyage

```bash
gcloud storage rm -r --all-versions gs://${BUCKET}
```

---

## Partie 7 — Cloud Run Avancé : Révisions & Traffic Splitting (20 min)

*Cloud Run gère des **révisions** (snapshots immuables d'un déploiement). On peut router le trafic entre plusieurs révisions pour des déploiements progressifs.*

### 7.1 — Déployer une nouvelle révision

```bash
PROJECT_ID=$(gcloud config get-value project)
IMAGE="europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tp2-app:v1"

# Déployer une "v2" avec une variable d'environnement différente
# --no-traffic : la nouvelle révision ne reçoit PAS de trafic immédiatement
gcloud run deploy tp2-service \
  --image=${IMAGE} \
  --region=europe-west9 \
  --no-traffic \
  --set-env-vars="APP_ENV=production,APP_VERSION=2.0.0" \
  --tag=canary

# Lister les révisions
gcloud run revisions list \
  --service=tp2-service \
  --region=europe-west9
```

### 7.2 — Traffic Splitting (déploiement Canary)

```bash
# Récupérer les noms des 2 révisions
REV_STABLE=$(gcloud run revisions list \
  --service=tp2-service --region=europe-west9 \
  --format="value(name)" | sed -n '2p')

REV_CANARY=$(gcloud run revisions list \
  --service=tp2-service --region=europe-west9 \
  --format="value(name)" | sed -n '1p')

# Diviser le trafic : 80% stable, 20% canary
gcloud run services update-traffic tp2-service \
  --region=europe-west9 \
  --to-revisions="${REV_STABLE}=80,${REV_CANARY}=20"

# Résultat :
# 80% tp2-service-00001-vtb
# 20% tp2-service-00002-kof

# Vérifier la répartition du trafic
gcloud run services describe tp2-service \
  --region=europe-west9 \
  --format="yaml(status.traffic)"
```

### 7.3 — Tester la répartition

```bash
SERVICE_URL=$(gcloud run services describe tp2-service \
  --region=europe-west9 --format='value(status.url)')

# Envoyer 10 requêtes et observer quelle version répond
for i in $(seq 1 10); do
  curl -s ${SERVICE_URL}/
done

# Sur 10 requêtes, environ 2 (20%) devraient être servies par la canary
# et 8 (80%) par la stable.
# Note : les deux révisions utilisent la même image ici, donc la réponse
# est identique — en production on aurait un code différent pour distinguer.
```

### 7.4 — Basculer 100% vers la nouvelle révision (promotion)

```bash
# Après validation : envoyer 100% du trafic vers la canary
gcloud run services update-traffic tp2-service \
  --region=europe-west9 \
  --to-latest

# Vérifier
gcloud run services describe tp2-service \
  --region=europe-west9 \
  --format="yaml(status.traffic)"
# 100% tp2-service-00002-vit (canary promue)
```

**Question :** Pourquoi le traffic splitting est-il préférable à un redéploiement direct (`--to-latest` immédiat) en production ?

> **Réponse :** Un redéploiement direct bascule 100% des utilisateurs vers la nouvelle version d'un coup — si un bug se glisse en production, tout le monde en est affecté immédiatement.
>
> Le traffic splitting permet un **déploiement canary** : on expose la nouvelle version à une fraction du trafic (ex: 20%) pour valider son comportement en conditions réelles. Si une anomalie est détectée, on reroute instantanément 100% du trafic vers la version stable, sans downtime. Ce n'est qu'après validation qu'on bascule complètement.

---

## Partie 8 — Docker Compose : Ajouter un Cache Redis (20 min)

*Les applications cloud utilisent souvent un cache en mémoire pour réduire la charge sur la base de données et accélérer les réponses.*

### 8.1 — Ajouter Redis au docker-compose.yml

Service Redis ajouté dans `docker-compose.yml` :

```yaml
  cache:
    image: redis:7-alpine
    command: redis-server --maxmemory 128mb --maxmemory-policy allkeys-lru
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3
    networks:
      - app-network
```

Variables d'environnement ajoutées au service `web` :

```yaml
    environment:
      - REDIS_HOST=cache
      - REDIS_PORT=6379
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
```

### 8.2 — Ajouter la route /cached dans l'application

Route `/cached` ajoutée dans `src/index.ts` avec :
- Lecture depuis Redis (cache hit → `source: "cache"` + TTL restant)
- Si cache miss → lecture depuis PostgreSQL, stockage dans Redis avec TTL de 10 secondes

```bash
# redis v4+ inclut ses propres types TypeScript
npm install redis
```

### 8.3 — Tester le cache

```bash
docker compose up -d --build

# Créer une visite via /db
curl http://localhost:8080/db
# {"total_visits":2}

# Première requête → source: "database" (cache froid)
curl http://localhost:8080/cached
# {"total_visits":2,"source":"database"}

# Requêtes suivantes dans les 10 secondes → source: "cache"
curl http://localhost:8080/cached
# {"total_visits":2,"source":"cache","ttl_remaining":10}

curl http://localhost:8080/cached
# {"total_visits":2,"source":"cache","ttl_remaining":10}

# Attendre 11 secondes et relancer → source: "database" (cache expiré)
sleep 11 && curl http://localhost:8080/cached
# {"total_visits":2,"source":"database"}
```

> **Quel est l'intérêt du TTL (Time-To-Live) dans un cache ?** Le TTL garantit que les données en cache expirent automatiquement après un délai défini, forçant un rafraîchissement depuis la source de vérité (la base de données). Sans TTL, le cache pourrait servir des données obsolètes indéfiniment.

**Question :** Dans quelle situation l'utilisation d'un cache Redis peut-elle poser un problème de cohérence des données ?

> **Réponse :** Quand la base de données est modifiée (INSERT, UPDATE, DELETE) pendant que le cache contient encore une ancienne valeur. Jusqu'à expiration du TTL, Redis sert des données périmées qui ne reflètent plus l'état réel de la base.
>
> Exemple concret : un utilisateur appelle `/db` (total passe à 4 en base), mais `/cached` continue de retourner `total_visits: 3` pendant les 10 secondes restantes du TTL.
>
> Pour limiter ce problème, on peut invalider manuellement le cache à chaque écriture (**cache-aside pattern**), ou réduire le TTL au prix de plus de requêtes vers la base.

---

## Nettoyage Final

```bash
# Supprimer le service Cloud Run
gcloud run services delete tp2-service --region=europe-west9 --quiet

# Supprimer le repository Artifact Registry (et toutes les images)
gcloud artifacts repositories delete tp2-registry \
  --location=europe-west9 --quiet

# Vérification
gcloud run services list --region=europe-west9
gcloud artifacts repositories list --location=europe-west9
```
