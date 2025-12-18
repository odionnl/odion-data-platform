with

source as (

    select * from {{ source('ons_plan_2', 'nexus_client_contact_relation_types') }}

),

renamed as (

    select
        objectId as contactpersoon_relatietype_id,
        name as contactpersoon_relatietype,
        categoryObjectId as contactpersoon_relatietype_categorie_id,
        active as is_actief
    from source

)

select 
    contactpersoon_relatietype_id,
    contactpersoon_relatietype,
    contactpersoon_relatietype_categorie_id,
    is_actief
from renamed