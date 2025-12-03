with

source as (

    select * from {{ source('ons_plan_2', 'careplan_domain_definitions') }}

),

renamed as (

    select
        objectId as domein_id,
        name as domein_naam,
        hidden as verborgen,
        createdAt as aangemaakt_op,
        updatedAt as bewerkt_op

    from source

)

select 
    domein_id,
    domein_naam,
    verborgen,
    aangemaakt_op,
    bewerkt_op
from renamed

