with

source as (

    select * from {{ source('ons_plan_2', 'status_updates') }}

),

renamed as (

    select
        id as status_update_id,
        medication_administration_id as toediening_id,
        employee_id as werknemer_id,
        [to] as nieuwe_status,
        reason as reden,
        created_at as aangemaakt_op
    from source

)

select 
    status_update_id,
    toediening_id,
    werknemer_id,
    nieuwe_status,
    reden,
    aangemaakt_op
from renamed