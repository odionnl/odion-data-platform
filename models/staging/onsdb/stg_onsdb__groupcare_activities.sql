with bron as (

    select * from {{ source('ons_plan_2', 'groupcare_activities') }}

),

definitief as (

    select
        id              as groepszorg_activiteit_id,
        external_id     as activiteit_id  -- ObjectId in Ons Administratie (→ stg_onsdb__activities)

    from bron

)

select * from definitief
