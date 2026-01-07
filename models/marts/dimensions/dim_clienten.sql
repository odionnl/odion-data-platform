with src as (

    select * from {{ ref('int_clients') }}

)

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
    src.naam,
    src.leeftijd,
    src.leeftijdsgroep,
    src.in_zorg

    from src
;
