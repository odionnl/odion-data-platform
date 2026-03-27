with bron as (

    select * from {{ source('ons_plan_2', 'status_updates') }}

),

definitief as (

    select
        id                              as statusupdate_id,
        medication_administration_id    as toediening_id,
        [to]                            as status,
        created_at                      as aangemaakt_op

    from bron

)

select * from definitief
