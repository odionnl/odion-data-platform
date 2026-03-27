with bron as (

    select * from {{ source('ons_plan_2', 'costcenter_assignments') }}

),

definitief as (

    select
        objectId            as kostenplaatskoppeling_id,
        unitObjectId        as locatie_id,
        costcenterObjectId  as kostenplaats_id,
        cast(beginDate as date) as startdatum,
        cast(endDate as date)   as einddatum

    from bron

)

select * from definitief
