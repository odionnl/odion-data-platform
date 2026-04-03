# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

dbt project voor **Odion** (zorgorganisatie), met als doel Power BI-rapportages over cliënten, medewerkers, locaties en verantwoording feitelijk geleverde zorg.

- **Database:** SQL Server (`Ons_Plan_2`), T-SQL dialect
- **dbt adapter:** `dbt-sqlserver ^1.9.0`
- **dbt versie:** `>=1.7.0, <2.0.0`
- **Python:** `>=3.10,<3.13` via Poetry
- **dbt package:** `dbt-labs/dbt_utils >=1.1.0,<2.0.0`

## Veelgebruikte commando's

Alle commando's uitvoeren vanuit de project root (zodat `.dlt/secrets.toml` gevonden wordt):

```bash
# dbt — modellen bouwen
poetry run dbt run                          # alle modellen
poetry run dbt run --select staging         # alleen staging laag
poetry run dbt run --select marts           # alleen marts
poetry run dbt run --select mart_clienten   # één model

# dbt — testen
poetry run dbt test                         # alle tests
poetry run dbt test --select mart_clienten  # tests voor één model

# dbt — overig
poetry run dbt compile                      # compileer SQL zonder te draaien
poetry run dbt show --select mart_clienten  # preview resultaat (eerste 5 rijen)
poetry run dbt docs generate && poetry run dbt docs serve

# Python ingestion pipelines
poetry run python -m ingestion.pipelines.ortec
poetry run python -m ingestion.pipelines.ons_audits

# Dagelijkse volledige pipeline (PowerShell)
./run_pipeline.ps1
```

## Architectuur

### Lagen (schema's)

| Laag | Schema | Materialisatie | Naamgeving |
|---|---|---|---|
| Staging | `staging` | view | `stg_<bron>__<tabel>` |
| Intermediate | `intermediate` | view | `int_<entiteit>_<omschrijving>` |
| Intermediate actueel | `intermediate` | view | `int_<entiteit>_actueel` |
| Verantwoording | `intermediate` | view | `int_check_<onderwerp>` |
| Marts | `marts` | table | `mart_<onderwerp>` |
| Marts actueel | `marts` | view | `mart_<onderwerp>_actueel` |

### Databronnen

- **`ons_plan_2`** (OnsDB via SQL Server `dbo`): alle zorgdata — 34 staging modellen
- **`raw_ortec`**: dienstroosters ORTEC — geladen via `ingestion/pipelines/ortec.py`
- **`raw_ons_audits`**: RPA-audits zorgplan-inzage — geladen via `ingestion/pipelines/ons_audits.py`

Ingestion gebruikt **dlt** (`dlt[mssql]`). Credentials staan in `.dlt/secrets.toml` (git-ignored).

### Tijdsperspectief-conventie

- **Staging / Intermediate basis/hierarchie**: geen datumfilter (all-time)
- **`int_*_actueel`**: gefilterd op `CAST(GETDATE() AS DATE)` (snapshot vandaag)
- **`int_check_*`**: `CAST(GETDATE() AS DATE)` en/of `DATEADD(day, -28, CAST(GETDATE() AS DATE))`
- **Marts (base)**: all-time, met `is_actief` / `is_in_zorg` kolom berekend op `GETDATE()`
- **Marts actueel (`mart_*_actueel`)**: view op base mart met `WHERE is_actief = 1` — snapshot vandaag
- **Uitzondering**: `mart_feitelijk_geleverde_zorg` is inherent actueel (alleen cliënten in zorg)

Regel: gebruik altijd `GETDATE()` direct — **geen** dbt-variabelen of macros voor datumfilters.

### Macros (`macros/`)

| Macro | Bestand | Gebruik |
|---|---|---|
| `get_locatiecluster(locatienaam, niveau2, niveau3)` | `locaties.sql` | CC-specifieke clusterindeling op naam/hiërarchie |
| `get_leeftijdsgroep(leeftijd_col)` | `leeftijdsgroep.sql` | `<18`, `18-49`, `50-64`, `65+`, `Onbekend` |
| `ons_dossier_url(path, client_id)` | `ons_urls.sql` | Deep-link naar ONS-dossier pagina |
| `ons_administratie_url(client_id)` | `ons_urls.sql` | Deep-link naar ONS-administratie |

## Naamgevingsconventies

- **PK staging**: `<entiteit_enkelvoud>_id` (bv. `client_id`, `locatie_id`, `medewerker_id`)
- **FK kolommen**: `<gerefereerde_tabel>_id`
- **Datums**: `startdatum` / `einddatum` / `aangemaakt_op` / `gewijzigd_op`
- **Booleans**: `is_*` als integer (0/1)
- **Intermediate actueel**: suffix `_actueel` voor snapshot-modellen (gefilterd op vandaag)

## OnsDB schema-bevindingen

- Geen soft-delete kolom (`deletedAt`) op de tabellen die we gebruiken — gebruik datumfilters
- `care_allocations` bevat: `outOfCareReason`, `outOfCareDestination`, `comments`
- `finance_types` bevat: `id`, `description`, `category`, `financeTypeGroup`, `beginDate`, `endDate`
- `clients` bevat: `dateOfBirth`, geen `deletedAt`

## Documentatiestructuur

Tests en beschrijvingen staan in YAML-bestanden naast de modellen:

- `models/staging/onsdb/_onsdb__sources.yml` + `_onsdb__models.yml`
- `models/staging/ortec/_ortec__sources.yml` + `_ortec__models.yml`
- `models/staging/ons_audits/_ons_audits__sources.yml` + `_ons_audits__models.yml`
- `models/intermediate/_int__models.yml`
- `models/intermediate/verantwoording/_verantwoording__models.yml`
- `models/marts/_marts__models.yml`
