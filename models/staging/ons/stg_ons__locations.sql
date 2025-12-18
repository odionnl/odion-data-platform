with source as (

    select * 
    from {{ source('ons_plan_2', 'locations') }}

),

renamed as (

    select
        objectId                    as locatie_id,
        cast(beginDate as date)     as startdatum_locatie,
        cast(endDate as date)       as einddatum_locatie,
        name                        as locatienaam,
        parentObjectId              as ouder_locatie_id,
        materializedPath            as locatie_hierarchie_pad,
        addressObjectId             as adres_id
    from source

)

select
    locatie_id,
    startdatum_locatie,
    einddatum_locatie,
    locatienaam,
    ouder_locatie_id,
    locatie_hierarchie_pad,
    adres_id
from renamed;
