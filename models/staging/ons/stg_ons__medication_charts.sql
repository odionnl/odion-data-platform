with

source as (

    select * from {{ source('ons_plan_2', 'medication_charts') }}

),

renamed as (

    select
        id as toedienlijst_id,
        client_id as client_id,
        date as datum,
        fake as fake,
        generated_at as aangemaakt_op
    from source

)

select 
    toedienlijst_id,
    client_id,
    datum,
    fake,
    aangemaakt_op
from renamed