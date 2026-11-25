with bron as (

    select * from {{ source('ons_plan_2', 'onsagenda_clients') }}

),

definitief as (

    select
        objectId                as agenda_client_id,
        externalObjectId        as client_id  -- ObjectId in Ons Administratie (→ stg_onsdb__clients)

    from bron

)

select * from definitief
