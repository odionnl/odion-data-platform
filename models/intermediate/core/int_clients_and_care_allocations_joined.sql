{{ config(materialized='view') }}

with clients as (

    select * from {{ ref('int_clients') }}

),


care_allocations as (

    select * from {{ ref('int_care_allocations') }}

),


joined as (

    select
        clients.client_id,
        clients.clientnummer,
        clients.geboortedatum,
        clients.overlijdensdatum,
        clients.achternaam,
        clients.geboortenaam,
        clients.voornaam,
        clients.partnernaam,
        clients.initialen,
        clients.prefix,
        clients.naam,
        care_allocations.startdatum_zorg,
        care_allocations.einddatum_zorg,
        care_allocations.reden_uit_zorg
    from clients

    left join care_allocations
        on clients.client_id = care_allocations.client_id
)

select * from joined;
