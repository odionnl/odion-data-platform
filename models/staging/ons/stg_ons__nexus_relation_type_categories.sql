with

source as (

    select * from {{ source('ons_plan_2', 'nexus_relation_type_categories') }}

),

renamed as (

    select
        objectId as relatietype_categorie_id,
        name as relatietype_categorie
    from source

)

select 
    relatietype_categorie_id,
    relatietype_categorie
from renamed