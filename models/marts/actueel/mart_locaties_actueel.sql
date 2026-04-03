-- Snapshot: alleen locaties die vandaag actief zijn.
-- Dunne view op mart_locaties.

select * from {{ ref('mart_locaties') }}
where is_actief = 1
