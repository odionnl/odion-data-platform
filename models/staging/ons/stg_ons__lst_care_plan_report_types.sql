with

source as (

    select * from {{ source('ons_plan_2', 'lst_care_plan_report_types') }}

),

renamed as (

    select
        code as rapportage_type_code,
        description as rapportage_type

    from source

)

select 
    rapportage_type_code,
    rapportage_type
from renamed