with bron as (

    select * from {{ source('ons_plan_2', 'dossier_episodes') }}

),

definitief as (

    select
        objectId            as episode_id,
        clientObjectId      as client_id,
        title               as titel,
        startDate           as startdatum,
        endDate             as einddatum,
        evaluationDate      as evaluatiedatum,
        cast(goal as nvarchar(max)) as doel,
        cast(case when marked    = 1 then 1 else 0 end as bit) as is_gemarkeerd,
        cast(case when important = 1 then 1 else 0 end as bit) as is_belangrijk,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op,
        createdById         as aangemaakt_door_id,
        updatedById         as gewijzigd_door_id

    from bron

)

select * from definitief
