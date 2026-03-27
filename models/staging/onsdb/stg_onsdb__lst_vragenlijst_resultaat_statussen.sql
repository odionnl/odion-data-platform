with bron as (

    select * from {{ source('ons_plan_2', 'lst_survey_result_statuses') }}

),

definitief as (

    select
        code            as status_code,
        description     as status_omschrijving

    from bron

)

select * from definitief
