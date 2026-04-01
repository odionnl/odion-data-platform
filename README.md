# Odion Data Platform

dbt-project voor **Odion** dat data uit Nedap Ons, ORTEC en RPA-audits transformeert naar analytische modellen voor Power BI-rapportages over clienten, medewerkers, locaties en verantwoording feitelijk geleverde zorg.

## Tech stack

| Component | Versie |
|---|---|
| dbt-core | `>=1.7.0, <2.0.0` |
| dbt-sqlserver | `^1.9.0` |
| Python | `>=3.10, <3.13` |
| Database | SQL Server (T-SQL) |
| Package manager | Poetry |
| Ingestion | dlt (`dlt[mssql]`) |

## Projectstructuur

```
odion-data-platform/
├── models/
│   ├── staging/          # Views — hernoemen & typen van brondata
│   ├── intermediate/     # Views — joins, filters, businesslogica
│   └── marts/            # Tables — eindmodellen voor Power BI
├── macros/               # Herbruikbare SQL-macros
├── analyses/             # Ad-hoc analysequery's
├── ingestion/            # Python data-ingestion pipelines (dlt)
│   └── pipelines/
├── run_pipeline.ps1      # Dagelijkse ETL + dbt run (PowerShell)
└── dbt_project.yml
```

## Databronnen

| Bron | Schema | Laadmethode |
|---|---|---|
| **Nedap Ons** (OnsDB) | `dbo` → `staging` | Directe SQL Server-verbinding via dbt |
| **ORTEC** (dienstroosters) | `raw_ortec` → `staging` | dlt-pipeline (`ingestion/pipelines/ortec.py`) |
| **RPA-audits** (zorgplan-inzage) | `raw_ons_audits` → `staging` | dlt-pipeline (`ingestion/pipelines/ons_audits.py`) |

## Installatie

### Vereisten

- Python 3.10–3.12
- [Poetry](https://python-poetry.org/docs/#installation)
- Toegang tot de SQL Server-database

### Setup

```bash
# Installeer dependencies
poetry install

# Installeer dbt-packages
poetry run dbt deps

# Configureer database-connectie
# Maak profiles.yml aan (git-ignored) — zie dbt docs:
# https://docs.getdbt.com/docs/core/connect-data-platform/mssql-setup

# Configureer ingestion credentials
# Maak .dlt/secrets.toml aan (git-ignored)
```

## Gebruik

### Dagelijkse pipeline (alles in een keer)

```powershell
./run_pipeline.ps1
```

Dit voert achtereenvolgens uit:
1. ORTEC-data laden
2. RPA-auditbestanden laden
3. `dbt run` (alle modellen)

### dbt-commando's

```bash
# Alle modellen bouwen
poetry run dbt run

# Specifieke laag
poetry run dbt run --select staging
poetry run dbt run --select intermediate
poetry run dbt run --select marts

# Eén model
poetry run dbt run --select <modelnaam>

# Tests draaien
poetry run dbt test

# Preview van modelresultaat
poetry run dbt show --select <modelnaam>

# Documentatie
poetry run dbt docs generate && poetry run dbt docs serve
```

### Ingestion pipelines los draaien

```bash
poetry run python -m ingestion.pipelines.ortec
poetry run python -m ingestion.pipelines.ons_audits
```

## Architectuur

### Modellagen

```
Brondata ──► Staging (views) ──► Intermediate (views) ──► Marts (tables) ──► Power BI
```

| Laag | Schema | Materialisatie | Naamgeving |
|---|---|---|---|
| Staging | `staging` | view | `stg_<bron>__<tabel>` |
| Intermediate | `intermediate` | view | `int_<entiteit>_<omschrijving>` |
| Marts | `marts` | table | `mart_<onderwerp>` |

Bekijk de volledige modeldocumentatie met `poetry run dbt docs generate && poetry run dbt docs serve`.

## Conventies

- **Primary keys**: `<entiteit_enkelvoud>_id` (bijv. `client_id`, `locatie_id`)
- **Foreign keys**: `<gerefereerde_tabel>_id`
- **Datums**: `startdatum`, `einddatum`, `aangemaakt_op`, `gewijzigd_op`
- **Booleans**: `is_*` als integer (0/1)
- **Datumfilters**: altijd `GETDATE()` — geen dbt-variabelen of macros
- **Intermediate actueel**: suffix `_actueel` voor snapshot-modellen gefilterd op vandaag
