{{ config(materialized='view') }}

with src as (

    select * from {{ ref('stg_ons__clients') }}

),


final as (

    select
        src.client_id,
        src.clientnummer,
        src.geboortedatum,
        src.overlijdensdatum,
        src.achternaam,
        src.geboortenaam,
        src.voornaam,
        src.partnernaam,
        src.initialen,
        src.prefix,
        src.naam
    from src
    where clientnummer is not null
        and clientnummer not in ('onbekend')
        and src.geboortedatum is not null

)

select * from final;
