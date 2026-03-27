with bron as (

    select * from {{ source('ons_plan_2', 'groupcare_locations') }}

),

definitief as (

    select
        id              as groepszorg_locatie_id,
        external_id     as locatie_id  -- ObjectId in Ons Administratie (→ stg_onsdb__locations)

    from bron

)

select * from definitief
