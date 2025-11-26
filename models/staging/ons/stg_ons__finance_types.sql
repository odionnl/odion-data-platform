with

source as (

    select * from {{ source('ons_plan_2', 'finance_types') }}

),

renamed as (

    select
        objectId as financieringstype_id,
        id as financieringstype_code,
        description as financieringstype_beschrijving,
        exportcode as financieringstype_exportcode
    from source

)

select 
    financieringstype_id,
    financieringstype_code,
    financieringstype_beschrijving,
    financieringstype_exportcode
from renamed