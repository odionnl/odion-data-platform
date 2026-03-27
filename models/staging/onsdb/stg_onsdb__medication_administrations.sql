with bron as (

    select * from {{ source('ons_plan_2', 'medication_administrations') }}

),

definitief as (

    select
        id                              as toediening_id,
        administration_agreement_id     as toedienafspraak_id,
        scheduled_at                    as ingepland_op,
        exempt                          as is_vrijgesteld

    from bron

)

select * from definitief
