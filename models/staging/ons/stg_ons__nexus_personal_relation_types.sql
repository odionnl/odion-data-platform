with

source as (

    select * from {{ source('ons_plan_2', 'nexus_personal_relation_types') }}

),

renamed as (

    select
        objectId as persoonlijke_relatietype_id,
        name as persoonlijke_relatietype,
        active as is_actief
    from source

)

select 
    persoonlijke_relatietype_id,
    persoonlijke_relatietype,
    is_actief
from renamed