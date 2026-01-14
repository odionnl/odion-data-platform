with

source as (

    select * from {{ source('ons_plan_2', 'administration_agreements') }}

),

renamed as (

    select
        id as toedienafspraak_id,
        medication_chart_id as toedienlijst_id,
        as_needed as zo_nodig,
        requires_double_check as vereist_dubbele_controle
    from source

)

select 
    toedienafspraak_id,
    toedienlijst_id,
    zo_nodig,
    vereist_dubbele_controle
from renamed