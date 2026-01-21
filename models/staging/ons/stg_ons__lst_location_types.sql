with

source as (

    select * from {{ source('ons_plan_2', 'lst_location_types') }}

),

renamed as (

    select
        code as locatie_type_code,
        description as locatie_type

    from source

)

select 
    locatie_type_code,
    locatie_type
from renamed