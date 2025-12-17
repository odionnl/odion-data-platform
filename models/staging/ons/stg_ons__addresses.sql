with

source as (

    select * from {{ source('ons_plan_2', 'addresses') }}

),

renamed as (

    select
        objectId as adres_id,
        street as straatnaam,
        homeNumber as huisnummer,
        city as woonplaats,
        municipality as gemeente,
        beginDate as startdatum_adres,
        endDate as einddatum_adres,
        type as adrestype_code
    from source

)

select 
    adres_id,
    straatnaam,
    huisnummer,
    woonplaats,
    gemeente,
    startdatum_adres,
    einddatum_adres,
    adrestype_code
from renamed