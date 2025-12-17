{{ config(materialized='view') }}

with params as (
    select cast(getdate() as date) as vandaag
),

clienten as (
    select
        c.*,
        p.vandaag
    from {{ ref('int_clients') }} c
    cross join params p
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
        c.clientnummer,
        c.in_zorg,
        l.locatie_id,
        l.locatienaam,
        la.startdatum_locatie,
        la.einddatum_locatie,
        la.locatie_type,
        l.locatie_pad,
        l.niveau1,
        l.niveau2,
        l.niveau3,
        l.niveau4,
        l.niveau5,
        l.niveau6
    from locatie_toewijzingen la
    inner join clienten c on la.client_id=c.client_id
    inner join locaties l on la.locatie_id=l.locatie_id
)

select *
from final;
