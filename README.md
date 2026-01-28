# Oil Regime Pipeline (MVP)

## Quick Start Guide

### Prereqs
- Python 3.9+
- PostgreSQL instance (local or cloud)
- EIA API key

### 1) Clone
```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

### 2) Configure environment
```bash
cp .env.example .env
```
Edit `.env`:
```
EIA_API_KEY=your_key
DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/db
```

### 3) Install dependencies
```bash
pip install -r requirements.txt
pip install python-dotenv
```

### 4) Run pipeline (in order)
```bash
python notebooks/01_bronze_ingest.py
python notebooks/02_silver_clean.py
python notebooks/03_gold_features.py
python notebooks/04_gold_to_postgres.py
```

### 5) (Optional) Run the API
```bash
uvicorn src.api.main:app --reload
```
Endpoints:
- `GET /health`
- `GET /signals/wti`
- `GET /regime/wti`
- `GET /forecast/wti`
