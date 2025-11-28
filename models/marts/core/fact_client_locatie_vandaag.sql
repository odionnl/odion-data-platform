{{ config(materialized='view') }}

with params as (
    select cast(getdate() as date) as vandaag
),

clienten as (
    select
        c.*,
        p.vandaag
    from {{ ref('int_clients_and_care_allocations_joined') }} c
    cross join params p
    where c.startdatum_zorg <= p.vandaag
      and c.einddatum_zorg > p.vandaag
),

locaties as (
    select * from {{ ref('int_location_hierarchy') }}
),

locatie_toewijzingen as (
    select
        la.*
    from {{ ref('int_location_assignments') }} la
    cross join params p
    where la.startdatum_locatie <= p.vandaag
      and la.einddatum_locatie > p.vandaag
),

final as (
    select
        c.client_id,
        l.locatie_id,
        la.startdatum_locatie,
        la.einddatum_locatie,
        la.locatie_type
    from locatie_toewijzingen la
    inner join clienten c on la.client_id=c.client_id
    inner join locaties l on la.locatie_id=l.locatie_id
)

select *
from final;
