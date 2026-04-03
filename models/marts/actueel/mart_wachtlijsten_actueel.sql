-- Snapshot: alleen actieve (lopende) wachtlijsten.
-- Dunne view op mart_wachtlijsten.

select * from {{ ref('mart_wachtlijsten') }}
where is_actief = 1
