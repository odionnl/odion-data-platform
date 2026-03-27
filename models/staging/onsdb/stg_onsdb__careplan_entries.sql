with bron as (

    select * from {{ source('ons_plan_2', 'careplan_entries') }}

),

definitief as (

    select
        objectId                    as zorgplanregel_id,
        carePlanObjectId            as zorgplan_id,
        linkId                      as zorgplanregel_link_id,
        targetDefinitionObjectId    as doeldefinitie_id,
        demandDefinitionObjectId    as aandachtspuntdefinitie_id,
        targetTitle                 as doel_titel,
        targetComment               as doel_opmerking,
        targetDate                  as streefdatum,
        percentageRealized          as percentage_gerealiseerd,
        createdAt                   as aangemaakt_op,
        updatedAt                   as gewijzigd_op

    from bron

)

select * from definitief
