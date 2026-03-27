with bron as (

    select * from {{ source('ons_plan_2', 'survey_results') }}

),

definitief as (

    select
        objectId                        as vragenlijst_resultaat_id,
        surveyObjectId                  as vragenlijst_id,
        clientObjectId                  as client_id,
        employeeObjectId                as medewerker_id,
        completedAt                     as ingevuld_op,
        status                          as status_code,
        readOnly                        as is_readonly,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
