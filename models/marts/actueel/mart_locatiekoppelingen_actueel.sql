-- Snapshot: alleen vandaag actieve cliënt-locatie koppelingen.
-- Bridge-view voor many-to-many cliënt ↔ locatie in Power BI.
-- Dunne view op mart_locatiekoppelingen.

select * from {{ ref('mart_locatiekoppelingen') }}
where is_actief = 1
