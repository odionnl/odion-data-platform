-- Snapshot: alleen vergelijkingsregels waarvan de locatieperiode of het traject
-- vandaag nog loopt. Dunne view op mart_jeugdhulp_trajecten_vergelijking.

select * from {{ ref('mart_jeugdhulp_trajecten_vergelijking') }}
where is_actief = 1
