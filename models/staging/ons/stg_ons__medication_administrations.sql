with

source as (

    select * from {{ source('ons_plan_2', 'medication_administrations') }}

),

renamed as (

    select
        id as toediening_id,
        administration_agreement_id as toedienafspraak_id,
        scheduled_at as gepland_op,
        exempt as vrijgesteld
    from source

)

select 
    toediening_id,
    toedienafspraak_id,
    gepland_op,
    vrijgesteld
from renamed