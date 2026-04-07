# TP2 App — Docker Avancé, Cloud Run & Networking GCP

Application Node.js/TypeScript déployée sur Google Cloud Run, avec une stack locale Docker Compose incluant PostgreSQL et Redis.

---

## Architecture

```
┌─────────────────────────────────────────┐
│              Docker Compose             │
│                                         │
│  ┌─────────┐   ┌──────────┐   ┌──────┐ │
│  │   web   │──▶│    db    │   │cache │ │
│  │ Node.js │   │PostgreSQL│   │Redis │ │
│  │ :8080   │──▶│  :5432   │   │:6379 │ │
│  └─────────┘   └──────────┘   └──────┘ │
│       │               app-network       │
└───────┼─────────────────────────────────┘
        │
        ▼
┌───────────────┐     ┌─────────────────────┐
│Artifact       │────▶│   Cloud Run         │
│Registry (GCP) │     │   tp2-service       │
└───────────────┘     │europe-west9.run.app │
                      └─────────────────────┘
```

### Services

| Service | Image | Rôle |
|---|---|---|
| `web` | Build local (multi-stage) | API Node.js/Express |
| `db` | `postgres:16-alpine` | Base de données PostgreSQL |
| `cache` | `redis:7-alpine` | Cache en mémoire (TTL 10s) |

### Routes

| Route | Description |
|---|---|
| `GET /` | Message de bienvenue + version |
| `GET /health` | Health check avec vérification DB |
| `GET /db` | Compteur de visites (écrit en base) |
| `GET /cached` | Compteur de visites avec cache Redis |

---

## Lancer la stack en local

```bash
# Démarrer tous les services
docker compose up -d --build

# Vérifier l'état des services
docker compose ps

# Tester les routes
curl http://localhost:8080/
curl http://localhost:8080/health
curl http://localhost:8080/db
curl http://localhost:8080/cached

# Voir les logs
docker compose logs -f

# Arrêter (données conservées)
docker compose down

# Arrêter et supprimer les volumes
docker compose down -v
```

---

## Build Docker

### Image naïve (sans multi-stage)

```bash
docker build -f Dockerfile.naive -t tp2-naive:v1 .
# Taille : ~202 MB
```

### Image multi-stage (production)

```bash
docker build -t tp2-app:v1 .
# Taille : ~139 MB — réduction de 31%
```

Le build multi-stage sépare la compilation (TypeScript) de l'exécution : seul le code compilé et les dépendances de production sont inclus dans l'image finale.

---

## Déploiement sur GCP

### 1. Pousser l'image sur Artifact Registry

```bash
PROJECT_ID=$(gcloud config get-value project)
IMAGE_TAG="europe-west9-docker.pkg.dev/${PROJECT_ID}/tp2-registry/tp2-app:v1"

gcloud auth configure-docker europe-west9-docker.pkg.dev
docker tag tp2-app:v1 ${IMAGE_TAG}
docker push ${IMAGE_TAG}
```

### 2. Déployer sur Cloud Run

```bash
gcloud run deploy tp2-service \
  --image=${IMAGE_TAG} \
  --region=europe-west9 \
  --platform=managed \
  --allow-unauthenticated \
  --port=8080 \
  --memory=512Mi \
  --cpu=1 \
  --max-instances=3 \
  --set-env-vars="APP_ENV=production"
```

### 3. Vérifier le déploiement

```bash
gcloud run services describe tp2-service --region=europe-west9
```

**URL publique :** `https://tp2-service-390464221655.europe-west9.run.app`

---

## Variables d'environnement

| Variable | Valeur par défaut | Description |
|---|---|---|
| `PORT` | `8080` | Port d'écoute |
| `DB_HOST` | `localhost` | Hôte PostgreSQL |
| `DB_PORT` | `5432` | Port PostgreSQL |
| `DB_NAME` | `ynov_db` | Nom de la base |
| `DB_USER` | `ynov` | Utilisateur PostgreSQL |
| `DB_PASSWORD` | `password` | Mot de passe PostgreSQL |
| `REDIS_HOST` | `localhost` | Hôte Redis |
| `REDIS_PORT` | `6379` | Port Redis |
| `APP_ENV` | — | Environnement (`development` / `production`) |
