with bron as (

    select * from {{ source('ons_plan_2', 'onsagenda_teams') }}

),

definitief as (

    select
        objectId                as agenda_team_id,
        externalObjectId        as team_id  -- ObjectId in Ons Administratie (→ stg_onsdb__teams)

    from bron

)

select * from definitief
