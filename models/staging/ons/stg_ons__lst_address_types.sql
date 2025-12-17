with

source as (

    select * from {{ source('ons_plan_2', 'lst_address_types') }}

),

renamed as (

    select
        code as adrestype_code,
        description as adrestype
    from source

)

select 
    adrestype_code,
    adrestype
from renamed