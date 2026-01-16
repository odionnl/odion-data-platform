# dbt + Linked Server (Azure SQL → SQL Server DWH)

## Doel
In dit dbt-project gebruiken we data uit een **externe Azure SQL-database (Ortec)** binnen ons **SQL Server Data Warehouse**, zonder deze data (nog) fysiek te kopiëren.  
Dit is gerealiseerd via een **SQL Server Linked Server** en **lokale views**, zodat dbt de data kan gebruiken alsof deze lokaal beschikbaar is.

Deze aanpak is bedoeld als **pragmatische tussenstap** richting een toekomstvaste ingestie-oplossing (bijv. via connectors zoals Airbyte of Azure Data Factory).

---

## Architectuuroverzicht (hoog niveau)

1. **Azure SQL (Ortec)**  
   Bevat brondata in schema `bi_support`.

2. **SQL Server DWH**  
   - Heeft een **Linked Server** (`ORTEC_BDP`) naar Azure SQL  
   - Bevat **views** in schema `src` die data ophalen via `OPENQUERY`

3. **dbt**  
   - Verbindt alleen met het DWH  
   - Gebruikt `source()` definitions die verwijzen naar `ORTEC_BDPReader.src.*`  
   - Bouwt staging- en verdere modellen bovenop deze views  

dbt maakt **geen directe verbinding** met Azure SQL.

---

## Waarom deze aanpak?

### Waarom geen directe dbt-verbinding met Azure SQL?
- dbt ondersteunt **slechts één databaseverbinding per run**
- dbt is een **transformation tool**, geen ingestie- of federatie-tool

### Waarom wel een Linked Server?
- Snel te realiseren
- Geen extra ingestietooling nodig
- Geschikt voor dagelijkse batch-analyses
- Handig voor exploratie en eerste integratie

### Waarom lokale views?
- dbt werkt het prettigst met **lokale tabellen/views**
- Complexiteit (linked server / `OPENQUERY`) blijft buiten dbt
- Later eenvoudig te vervangen door echte ingeladen tabellen
- Betere beheersbaarheid en leesbaarheid

---

## Implementatie in SQL Server

### Linked Server
Op de DWH SQL Server is een Linked Server aangemaakt:

- **Naam:** `ORTEC_BDP`
- **Type:** SQL Server → Azure SQL
- **Authenticatie:** SQL login (Azure SQL ondersteunt geen Windows-logins)

Deze Linked Server maakt het mogelijk om Azure SQL tabellen te benaderen via:
- 4-part naming  
- `OPENQUERY(...)`

---

### Views in het DWH

Voor elke benodigde Ortec-tabel is een **view** aangemaakt in schema `src`.

Voorbeeld:

```sql
USE ORTEC_BDPREADER;

CREATE OR ALTER VIEW src.fact_published_shift AS
SELECT *
FROM OPENQUERY(
  ORTEC_BDP,
  'SELECT * FROM bi_support.FACT_PUBLISHED_SHIFT'
);
```

## Gebruikte views

De volgende views zijn aangemaakt in het DWH onder schema `src` (passthrough naar Ortec/Azure SQL via Linked Server `ORTEC_BDP`):

- `src.fact_published_shift`
- `src.dim_cost_center`
- `src.dim_date`
- `src.dim_time`
- `src.dim_employee`

Deze views worden gebruikt als dbt-sources en vormen de “interface” tussen dbt en de externe Ortec-bron.

---
s
## Gebruik in dbt

### Sources (`sources.yml`)
Definieer de views als sources in dbt:

```yaml
sources:
  - name: ortec
    database: ORTEC_BDPReader
    schema: src
    tables:
      - name: fact_published_shift
      - name: dim_cost_center
      - name: dim_date
      - name: dim_time
      - name: dim_employee
```

## Gebruik in modellen

In dbt-modellen verwijs je vervolgens naar de sources met `source()`:

```sql
select *
from {{ source('ortec', 'fact_published_shift') }}
```

## Wanneer overstappen op ingestie?

Deze aanpak is bedoeld als tussenstap. Overstappen op ingestie (bijv. Azure Data Factory of Airbyte) is aan te raden wanneer:

- volumes groeien
- stabiliteit of SLA’s belangrijk worden
- backfills of herberekeningen nodig zijn
- governance en auditing vereist zijn

**Voordeel van deze opzet:**
Als later wordt overgestapt op ingestie, kunnen dezelfde namen (`src.*`) behouden blijven (als tabellen of views), waardoor dbt-modellen meestal niet hoeven te worden aangepast.