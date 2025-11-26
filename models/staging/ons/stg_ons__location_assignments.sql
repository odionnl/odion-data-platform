with

source as (

    select * from {{ source('ons_plan_2', 'location_assignments') }}

),

renamed as (

    select
        clientObjectId as client_id,
        locationObjectId as locatie_id,
        beginDate as startdatum_locatie,
        endDate as einddatum_locatie,
        locationType as locatie_type
    from source

)

select 
    client_id,
    locatie_id,
    startdatum_locatie,
    einddatum_locatie,
    locatie_type
from renamed