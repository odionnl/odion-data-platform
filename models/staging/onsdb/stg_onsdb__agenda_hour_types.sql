with bron as (

    select * from {{ source('ons_plan_2', 'agenda_hour_types') }}

),

definitief as (

    select
        objectId                as agenda_uursoort_id,
        externalObjectId        as activiteit_id  -- ObjectId in Ons Administratie (→ stg_onsdb__activities)

    from bron

)

select * from definitief
