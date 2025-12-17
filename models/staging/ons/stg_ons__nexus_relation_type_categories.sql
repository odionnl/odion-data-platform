with

source as (

    select * from {{ source('ons_plan_2', 'nexus_relation_type_categories') }}

),

renamed as (

    select
        objectId as relatie_type_categorie_id,
        name as relatie_type_categorie
    from source

)

select 
    relatie_type_categorie_id,
    relatie_type_categorie
from renamed