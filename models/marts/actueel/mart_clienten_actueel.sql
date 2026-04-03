-- Snapshot: alleen cliënten die vandaag in zorg zijn.
-- Dunne view op mart_clienten.

select * from {{ ref('mart_clienten') }}
where is_in_zorg = 1
