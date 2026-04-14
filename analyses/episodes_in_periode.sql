-- Export: episodes aangemaakt in periode

{% set startdatum = '2025-01-01' %}
{% set einddatum  = '2026-04-01' %}

select
  episode_id,
  client_id,
  clientnummer,
  titel as episode_titel,
  startdatum,
  einddatum,
  evaluatiedatum,
  doel,
  is_gemarkeerd,
  is_belangrijk,
  afgeschermd_voor,
  afgeschermd_voor_deskundigheden,
  afgeschermd_voor_deskundigheidsgroepen,
  medewerker_personeelsnummer,
  medewerker_deskundigheden,
  medewerker_deskundigheidsgroepen,
  aangemaakt_op,
  gewijzigd_op,
  year(aangemaakt_op)  as jaar,
  month(aangemaakt_op) as maand

from {{ ref('mart_episodes') }}

where aangemaakt_op >= '{{ startdatum }}'
  and aangemaakt_op  < '{{ einddatum }}'

order by aangemaakt_op
