with bron as (

    select * from {{ source('ons_plan_2', 'survey_answers') }}

),

definitief as (

    select
        objectId                        as vragenlijst_antwoord_id,
        surveyResultObjectId            as vragenlijst_resultaat_id,
        questionObjectId                as vragenlijst_vraag_id,
        answerDefinitionObjectId        as antwoord_definitie_id,
        [text]                          as tekst_antwoord,
        important                       as is_belangrijk,
        booleanAnswer                   as ja_nee_antwoord,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
