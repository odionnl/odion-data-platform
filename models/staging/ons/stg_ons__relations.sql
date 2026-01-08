with

source as (

    select * from {{ source('ons_plan_2', 'relations') }}

),

renamed as (

    select
        objectId as relatie_id,
        clientObjectId as client_id,
        firstName as voornaam,
        name as achternaam,
        personalRelationTypeId as persoonlijke_relatietype_id,
        clientContactRelationTypeId as contactpersoon_relatietype_id
    from source

)

select 
    relatie_id,
    client_id,
    voornaam,
    achternaam,
    persoonlijke_relatietype_id,
    contactpersoon_relatietype_id
from renamed