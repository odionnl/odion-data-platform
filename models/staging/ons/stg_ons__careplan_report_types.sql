with

source as (

    select * from {{ source('ons_plan_2', 'careplan_report_types') }}

),

renamed as (

    select
        objectId as rapportage_type_id,
        name as rapportage_type_naam,
        type as rapportage_type_code

    from source

)

select 
    rapportage_type_id,
    rapportage_type_naam,
    rapportage_type_code
from renamed