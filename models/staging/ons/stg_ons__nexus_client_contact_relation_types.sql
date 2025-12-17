with

source as (

    select * from {{ source('ons_plan_2', 'nexus_client_contact_relation_types') }}

),

renamed as (

    select
        objectId as client_contact_relatie_type_id,
        name as relatie_type,
        categoryObjectId as relatie_type_categorie_id
    from source

)

select 
    client_contact_relatie_type_id,
    relatie_type,
    relatie_type_categorie_id
from renamed