-- Export: rapportages aangemaakt in periode

{% set startdatum = '2025-01-01' %}
{% set einddatum  = '2026-04-01' %}

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
  gewijzigd_op,
  year(rapportagedatum)  as jaar,
  month(rapportagedatum) as maand

from {{ ref('mart_rapportages') }}

where rapportagedatum >= '{{ startdatum }}'
  and rapportagedatum  < '{{ einddatum }}'

order by rapportagedatum
