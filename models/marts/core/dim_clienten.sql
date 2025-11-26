{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('int_clients') }}
)

select
    -- sleutels & basis
    client_id,
    clientnummer,
    naam                 as client_naam,
    voornaam,
    achternaam,
    geboortenaam,
    partnernaam,
    initialen,
    prefix,

    geboortedatum,
    overlijdensdatum,

    -- flags & afgeleiden
    is_overleden,
    is_in_zorg_vandaag,
    leeftijd_vandaag,
    geboortejaar,
    geboortemaand,

    {{ get_leeftijdsgroep('leeftijd_vandaag') }} as leeftijdsgroep

from src;
