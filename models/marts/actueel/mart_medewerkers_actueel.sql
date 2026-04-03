-- Snapshot: alleen medewerkers met een actief contract vandaag.
-- Dunne view op mart_medewerkers.

select * from {{ ref('mart_medewerkers') }}
where is_actief = 1
