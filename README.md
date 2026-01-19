## Medical Telegram Data Warehouse

This project scrapes messages and images from selected medical and pharmaceutical Telegram channels, stores raw JSON in a local data lake, and builds a Postgres-backed analytical warehouse using dbt.

##  Features

- **Telegram Scraping** – Automated collection of messages and images from medical and healthcare Telegram channels  
- **Data Lake Storage** – Raw JSON files organized by channel and date  
- **Data Warehouse** – PostgreSQL analytics layer powered by dbt transformations  
- **Automated Pipeline** – End-to-end flow from scraping to analysis-ready tables  
- **Data Quality** – Built-in validation and testing at each stage  
- **Documentation** – Auto-generated dbt docs with lineage and metrics  




## Project Structure (summary)

```
medical-telegram-warehouse/
├── .vscode/
│   └── settings.json
├── .github/
│   └── workflows/
│       └── unittests.yml
├── .env               # Secrets (API keys, DB passwords) - DO NOT COMMIT
├── .gitignore
├── docker-compose.yml  # Container orchestration
├── Dockerfile          # Python environment
├── requirements.txt
├── README.md
├── data/
├── medical_warehouse/            # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
├── src/
├── api/
│   ├── __init__.py
│   ├── main.py                   # FastAPI application
│   ├── database.py               # Database connection
│   └── schemas.py                # Pydantic models
├── notebooks/
│   ├── __init__.py
├── tests/
│   └── __init__.py
└── scripts/
```

##  Prerequisites

- Python **3.8+**
- PostgreSQL **12+**
- Telegram API credentials (for scraping)
- Git

---

##  Installation

### 1. Clone the repository

```bash
git clone https://github.com/zerubabelalpha/medical-telegram-warehouse.git
cd medical-telegram-warehouse
## Quickstart

1. Create and activate a Python virtual environment:

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables (create `.env` in repo root):

```text
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_db_password
DB_NAME=medical_warehouse

# Telegram credentials for scraper (if using scraper)
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
TG_PHONE=+your_phone_number
```

4. Create the Postgres database (if needed):

```powershell
# from PowerShell (Postgres user must have privileges)
psql -U postgres -c "CREATE DATABASE medical_warehouse;"
```

5. Load raw JSON into Postgres:

```powershell
python scripts\load_to_postgres.py
```

6. dbt: install packages, build models, and run tests (from `medical_warehouse` directory):

```powershell
cd medical_warehouse
# install dbt packages listed in packages.yml
dbt deps
# build models
dbt run
# run dbt tests
dbt test
# generate and serve docs
dbt docs generate
dbt docs serve
```

Notes:
- `medical_warehouse/profiles.yml` configures dbt connection targets; you can also set connection values via environment variables.
- If you remove `medical_warehouse/target/` or `medical_warehouse/dbt_packages/`, restore them with `dbt run` and `dbt deps` respectively.



## Tests

- Python unit tests:

```powershell
python -m unittest discover tests
```

- dbt data tests are run with `dbt test` (see above). Custom SQL tests live in `medical_warehouse/tests/`.
