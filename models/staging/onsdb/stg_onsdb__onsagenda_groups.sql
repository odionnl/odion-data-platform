with bron as (

    select * from {{ source('ons_plan_2', 'onsagenda_groups') }}

),

definitief as (

    select
        objectId                as agenda_groep_id,
        externalObjectId        as groepszorg_groep_id  -- id in Groepsplanning (→ stg_onsdb__groupcare_groups)

    from bron

)

select * from definitief
