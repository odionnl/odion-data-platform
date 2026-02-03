with

source as (

    select * from {{ source('ons_plan_2', 'lst_pool_types') }}

),

renamed as (

    select
        code as pool_type_code,
        description as pool_type

    from source

)

select 
    pool_type_code,
    pool_type
from renamed