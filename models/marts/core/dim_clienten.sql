with src as (

    select * from {{ ref('int_clients') }}

),

final as(

select
    -- sleutels & basis
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
)

select * from final;
