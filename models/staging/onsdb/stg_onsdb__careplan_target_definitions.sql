with bron as (

    select * from {{ source('ons_plan_2', 'careplan_target_definitions') }}

),

definitief as (

    select
        objectId                as doeldefinitie_id,
        domainObjectId          as domein_definitie_id,
        name                    as doel_naam,
        allowComment            as is_opmerking_toegestaan,
        reportProgress          as is_voortgang_bijhouden,
        allowTitleOverride      as is_titel_aanpasbaar,
        hidden                  as is_verborgen,
        classificationId        as classificatie_id,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
