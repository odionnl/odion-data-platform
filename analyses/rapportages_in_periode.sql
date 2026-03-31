-- Export: rapportages aangemaakt in 2025
-- Direct uitvoerbaar in SSMS (geen dbt compile nodig)

declare @startdatum date = '2025-01-01';
declare @einddatum  date = '2026-01-01';

select
  rapportage_id,
  client_id,
  clientnummer,
  rapportage_type,
  rapportagedatum,
  is_gemarkeerd,
  is_verborgen,
  afgeschermd_voor,
  afgeschermd_voor_deskundigheden,
  afgeschermd_voor_deskundigheidsgroepen,
  medewerker_personeelsnummer,
  medewerker_deskundigheden,
  medewerker_deskundigheidsgroepen,
  aangemaakt_op,
  gewijzigd_op

from [OdionDataPlatform].[dbo].[mart_rapportages]

where rapportagedatum >= @startdatum
  and rapportagedatum  < @einddatum

order by rapportagedatum
