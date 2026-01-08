with

source as (

    select * from {{ source('ons_plan_2', 'nexus_personal_relation_types') }}

),

renamed as (

    select
        objectId as persoonlijke_relatietype_id,
        name as persoonlijke_relatietype,
        categoryObjectId as persoonlijke_relatietype_categorie_id,
        active as is_actief
    from source

)

select 
    persoonlijke_relatietype_id,
    persoonlijke_relatietype,
    persoonlijke_relatietype_categorie_id,
    is_actief
from renamed