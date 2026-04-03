-- Snapshot: alleen actieve zorglegitimatie-producten.
-- Dunne view op mart_zorglegitimatie_producten.

select * from {{ ref('mart_zorglegitimatie_producten') }}
where is_actief = 1
