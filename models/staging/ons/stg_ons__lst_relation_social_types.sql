with

source as (

    select * from {{ source('ons_plan_2', 'lst_relation_social_types') }}

),

renamed as (

    select
        code as relatie_sociaal_type_id,
        description as relatie_sociaal_type
    from source

)

select 
    relatie_sociaal_type_id,
    relatie_sociaal_type
from renamed