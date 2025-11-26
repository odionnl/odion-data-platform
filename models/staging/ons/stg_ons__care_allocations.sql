with

source as (

    select * from {{ source('ons_plan_2', 'care_allocations') }}

),

renamed as (

    select
        objectId as zorgtoewijzing_id,
        clientObjectId as client_id,
        dateBegin as startdatum_zorg,
        dateEnd as einddatum_zorg,
        outOfCareReason as reden_uit_zorg,
        comments as opmerkingen
    from source

)

select 
    zorgtoewijzing_id,
    client_id,
    startdatum_zorg,
    einddatum_zorg,
    reden_uit_zorg,
    opmerkingen
from renamed