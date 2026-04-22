with bron as (

    select * from {{ source('ons_plan_2', 'groupcare_activity_timelines') }}

),

definitief as (

    select
        timeline_id     as groupcare_afspraak_id,       -- → stg_onsdb__groupcare_registrations.groupcare_afspraak_id
        activity_id     as groepszorg_activiteit_id     -- → stg_onsdb__groupcare_activities.groepszorg_activiteit_id

    from bron

)

select * from definitief
