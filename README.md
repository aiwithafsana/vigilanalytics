# Vigil — Medicare Fraud Intelligence Platform

AI-powered investigation workbench for government fraud prosecutors. Scores 533,000+ Medicare providers using XGBoost, Isolation Forests, and Autoencoders. Detects billing anomalies, maps kickback networks, and generates prosecution-ready evidence packages.

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Node.js 20+
- Python 3.12+
- PostgreSQL 16 (or use the Docker service below)

---

### 1. Start PostgreSQL

```bash
cd infra
docker compose up db -d
```

---

### 2. Backend

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # Edit SECRET_KEY — generate with: openssl rand -hex 32
uvicorn app.main:app --reload
```

API runs at **http://localhost:8000** · Docs at **http://localhost:8000/docs**

---

### 3. Seed Database

```bash
cd db
python seed.py
```

Creates two users and two demo providers. Idempotent — safe to run multiple times.

---

### 4. Frontend

```bash
cd frontend
npm install
npm run dev
```

App runs at **http://localhost:3000**

---

### Login Credentials

| Role  | Email                  | Password            |
|-------|------------------------|---------------------|
| Admin | admin@vigil.gov        | VigilAdmin2024!     |
| Analyst (FL/TX only) | analyst@vigil.gov | VigilAnalyst2024! |

---

### 5. ML Pipeline (optional — populates real CMS data)

Populates the database with 533,000+ real Medicare providers from CMS Part B 2022 public data.

```bash
cd ml
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run full pipeline (~15–30 min depending on hardware)
python run_pipeline.py --skip-hcpcs    # faster — skips entropy features

# Or with all features (requires downloading 1.5GB HCPCS file)
python run_pipeline.py
```

Pipeline steps:
1. `ingest.py` — Downloads CMS Part B (~200MB) + LEIE exclusions, parses them
2. `features.py` — Computes peer medians, z-scores, ratios, billing entropy
3. `train.py` — Trains Isolation Forest, XGBoost, Autoencoder
4. `score.py` — Scores all providers with composite 0–100 risk score
5. `flags.py` — Generates plain-English anomaly flags per provider
6. `load_db.py` — Upserts everything into PostgreSQL + computes dashboard stats

After pipeline completes, seed the referral network:

```bash
python -m pipeline.referrals
```

---

## Docker (Full Stack)

```bash
cd infra
docker compose up --build
```

Services: `db` (PostgreSQL 16), `backend` (FastAPI), `frontend` (Next.js)

---

## Architecture

```
vigil/
├── backend/          FastAPI — REST API, auth, RBAC, audit logging
│   └── app/
│       ├── routers/  providers, cases, dashboard, network, users, audit
│       ├── models.py SQLAlchemy models
│       ├── auth.py   JWT + bcrypt + role/state enforcement
│       └── services/ evidence PDF generation (ReportLab)
├── frontend/         Next.js 14 app router
│   └── app/
│       ├── dashboard/
│       ├── providers/
│       ├── cases/
│       ├── network/  D3 force-directed referral graph
│       └── admin/    User management
├── ml/               Python ML pipeline
│   └── pipeline/     ingest → features → train → score → flags → load_db
├── db/               Seed script
└── infra/            Docker Compose
```

## Security

- JWT access tokens (60 min) + refresh tokens (30 days)
- bcrypt password hashing
- RBAC: `admin` / `analyst` / `viewer` with state-level access filters
- Rate limiting: 10 req/min on login, 30/min on token refresh
- OWASP security headers (CSP, HSTS, X-Frame-Options, etc.)
- File upload validation: MIME whitelist, 25MB cap, UUID-only storage paths
- Full audit log on every read/write action
