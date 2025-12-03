with

source as (

    select * from {{ source('ons_plan_2', 'careplan_entries') }}

),

renamed as (

    select
        objectId as zorgplanregel_id,
        careplanObjectId as zorgplan_id,
        linkId as link_id,
        percentageRealized AS percentage_gerealiseerd,
        targetDefinitionObjectId as doel_definitie_id,
        targetComment as doel_opmerkingen,
        targetDate as doel_datum,
        targetTitle as doel_titel,
        demandComment as aandachtspunt_opmerkingen,
        demandDefinitionObjectId as aandachtspunt_id,
        createdAt as aangemaakt_op,
        updatedAt as bewerkt_op

    from source

)

select 
    zorgplanregel_id,
    zorgplan_id,
    link_id,
    percentage_gerealiseerd,
    doel_definitie_id,
    doel_opmerkingen,
    doel_datum,
    doel_titel,
    aandachtspunt_opmerkingen,
    aandachtspunt_id,
    aangemaakt_op,
    bewerkt_op
from renamed