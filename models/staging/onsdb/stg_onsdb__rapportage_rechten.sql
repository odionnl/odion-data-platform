with bron as (

    select * from {{ source('ons_plan_2', 'careplan_report_action_rights') }}

),

definitief as (

    select
        careplanReportObjectId      as rapportage_id,
        type                        as type_code,
        educationObjectId           as deskundigheid_id,
        expertiseGroupObjectId      as deskundigheidsgroep_id

    from bron

)

select * from definitief
