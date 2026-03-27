with bron as (

    select * from {{ source('ons_plan_2', 'survey_group_answers') }}

),

definitief as (

    select
        objectId                        as vragenlijst_groep_antwoord_id,
        surveyResultObjectId            as vragenlijst_resultaat_id,
        groupObjectId                   as vragenlijst_groep_id,
        important                       as is_belangrijk,
        score                           as score,
        version                         as versie,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
