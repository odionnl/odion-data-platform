with bron as (

    select * from {{ source('ons_plan_2', 'onsagenda_employees') }}

),

definitief as (

    select
        objectId                as agenda_medewerker_id,
        externalObjectId        as medewerker_id  -- ObjectId in Ons Administratie (→ stg_onsdb__employees)

    from bron

)

select * from definitief
