with bron as (

    select * from {{ source('ons_plan_2', 'lst_careplan_report_action_right_types') }}

),

definitief as (

    select
        code                        as type_code,
        description                 as type_omschrijving

    from bron

)

select * from definitief
