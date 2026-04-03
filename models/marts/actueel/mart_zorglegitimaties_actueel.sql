-- Snapshot: alleen actieve zorglegitimaties.
-- Dunne view op mart_zorglegitimaties.

select * from {{ ref('mart_zorglegitimaties') }}
where is_actief = 1
