with bron as (

    select * from {{ source('ons_plan_2', 'surveys') }}

),

definitief as (

    select
        objectId                        as vragenlijst_id,
        title                           as titel,
        description                     as beschrijving,
        active                          as is_actief,
        useStrictEditAuthorization      as gebruik_strikte_autorisatie,
        useWorkflow                     as gebruik_workflow,
        copyingAllowed                  as kopieren_toegestaan,
        classificationId                as classificatie_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
