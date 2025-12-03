with

source as (

    select * from {{ source('ons_plan_2', 'careplan_target_definitions') }}

),

renamed as (

    select
        objectId as doel_definitie_id,
        domainObjectId as domein_id,
        name as doel_naam,
        demandDefinitionObjectId as aandachtspunt_id,
        hidden as verborgen,
        createdAt as aangemaakt_op,
        updatedAt as bewerkt_op

    from source

)

select 
    doel_definitie_id,
    domein_id,
    doel_naam,
    aandachtspunt_id,
    verborgen,
    aangemaakt_op,
    bewerkt_op
from renamed

