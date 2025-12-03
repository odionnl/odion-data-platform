with

source as (

    select * from {{ source('ons_plan_2', 'careplan_demand_definitions') }}

),

renamed as (

    select
        objectId as aandachtspunt_id,
        domainObjectId as domein_id,
        name as aandachtspunt_naam,
        hidden as verborgen,
        createdAt as aangemaakt_op,
        updatedAt as bewerkt_op

    from source

)

select 
    aandachtspunt_id,
    domein_id,
    aandachtspunt_naam,
    verborgen,
    aangemaakt_op,
    bewerkt_op
from renamed

