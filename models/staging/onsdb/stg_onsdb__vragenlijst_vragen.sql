with bron as (

    select * from {{ source('ons_plan_2', 'survey_questions') }}

),

definitief as (

    select
        objectId                            as vragenlijst_vraag_id,
        groupObjectId                       as vragenlijst_groep_id,
        sequenceNumber                      as volgnummer,
        answerType                          as antwoord_type,
        [text]                              as vraagtekst,
        additionalInfo                      as aanvullende_info,
        defaultAnswer                       as standaard_antwoord,
        inactive                            as is_inactief,
        required                            as is_verplicht,
        answerDefinitionGroupObjectId       as antwoord_definitie_groep_id,
        classificationId                    as classificatie_id,
        createdAt                           as aangemaakt_op,
        updatedAt                           as gewijzigd_op

    from bron

)

select * from definitief
