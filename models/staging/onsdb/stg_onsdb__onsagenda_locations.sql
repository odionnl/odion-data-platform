with bron as (

    select * from {{ source('ons_plan_2', 'onsagenda_locations') }}

),

definitief as (

    select
        objectId                as agenda_locatie_id,
        externalObjectId        as locatie_id  -- ObjectId in Ons Administratie (→ stg_onsdb__locations)

    from bron

)

select * from definitief
