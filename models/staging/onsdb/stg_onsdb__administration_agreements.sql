with bron as (

    select * from {{ source('ons_plan_2', 'administration_agreements') }}

),

definitief as (

    select
        id                      as toedienafspraak_id,
        medication_chart_id

    from bron

)

select * from definitief
