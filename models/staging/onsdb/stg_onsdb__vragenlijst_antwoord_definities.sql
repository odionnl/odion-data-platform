with bron as (

    select * from {{ source('ons_plan_2', 'survey_answer_definitions') }}

),

definitief as (

    select
        objectId                            as antwoord_definitie_id,
        answerDefinitionGroupObjectId       as antwoord_definitie_groep_id,
        definition                          as antwoord_tekst,
        value                               as waarde,
        score                               as score,
        hasInput                            as heeft_tekstveld,
        classificationId                    as classificatie_id,
        createdAt                           as aangemaakt_op,
        updatedAt                           as gewijzigd_op

    from bron

)

select * from definitief
