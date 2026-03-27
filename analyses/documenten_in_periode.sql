-- Export: documenten aangemaakt in 2025
-- Direct uitvoerbaar in SSMS (geen dbt compile nodig)

declare @startdatum date = '2025-01-01';
declare @einddatum  date = '2026-01-01';

select
  document_id,
  client_id,
  clientnummer,
  beschrijving,
  bestandsnaam,
  documentstatus,
  labels,
  is_vertrouwelijk,
  afgeschermd_voor,
  afgeschermd_voor_deskundigheden,
  afgeschermd_voor_deskundigheidsgroepen,
  medewerker_personeelsnummer,
  medewerker_deskundigheden,
  medewerker_deskundigheidsgroepen,
  aangemaakt_op,
  gewijzigd_op

from [OdionDataPlatformCC].[dbo_marts].[mart_documenten]

where aangemaakt_op >= @startdatum
  and aangemaakt_op  < @einddatum

order by aangemaakt_op
