with bron as (

    select * from {{ source('ons_plan_2', 'survey_categories') }}

),

definitief as (

    select
        objectId                        as vragenlijst_categorie_id,
        surveyObjectId                  as vragenlijst_id,
        title                           as titel,
        sequenceNumber                  as volgnummer,
        classificationId                as classificatie_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
