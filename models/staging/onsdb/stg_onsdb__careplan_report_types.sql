with bron as (

    select * from {{ source('ons_plan_2', 'careplan_report_types') }}

),

definitief as (

    select
        objectId    as rapportage_type_id,
        name        as rapportage_type

    from bron

)

select * from definitief
