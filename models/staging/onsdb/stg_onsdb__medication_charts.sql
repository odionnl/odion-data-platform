with bron as (

    select * from {{ source('ons_plan_2', 'medication_charts') }}

),

definitief as (

    select
        id              as medication_chart_id,
        client_id,
        date            as datum,
        generated_at    as gegenereerd_op,
        fake            as is_nep

    from bron

)

select * from definitief
