with

source as (

    select * from {{ source('ons_plan_2', 'lst_care_plan_statuses') }}

),

renamed as (

    select
        code as zorgplan_status_code,
        description as zorgplan_status

    from source

)

select 
    zorgplan_status_code,
    zorgplan_status
from renamed