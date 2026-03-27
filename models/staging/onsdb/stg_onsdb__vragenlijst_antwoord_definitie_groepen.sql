with bron as (

    select * from {{ source('ons_plan_2', 'survey_answer_definition_groups') }}

),

definitief as (

    select
        objectId                        as antwoord_definitie_groep_id,
        description                     as beschrijving,
        readonly                        as is_readonly,
        classificationId                as classificatie_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
