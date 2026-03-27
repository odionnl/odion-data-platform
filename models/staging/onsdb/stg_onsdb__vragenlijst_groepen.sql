with bron as (

    select * from {{ source('ons_plan_2', 'survey_groups') }}

),

definitief as (

    select
        objectId                        as vragenlijst_groep_id,
        categoryObjectId                as vragenlijst_categorie_id,
        description                     as beschrijving,
        sequenceNumber                  as volgnummer,
        demandObjectId                  as aandachtspunt_id,
        thresholdScore                  as drempelwaarde_score,
        maxScore                        as max_score,
        classificationId                as classificatie_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
