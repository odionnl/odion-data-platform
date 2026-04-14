-- Snapshot: alleen kamers met een actieve cliënt-koppeling vandaag.
-- Dunne view op mart_kamerbezetting.

select * from {{ ref('mart_kamerbezetting') }}
where is_actieve_koppeling = 1
