with bron as (

    select * from {{ source('ons_plan_2', 'dossier_episode_actions') }}

),

definitief as (

    select
        objectId                    as episode_actie_id,
        subGoalObjectId             as episode_subdoel_id,
        title                       as titel,
        cast(case when showInCarePlan = 1 then 1 else 0 end as bit) as tonen_in_zorgplan,
        cast(case when archived      = 1 then 1 else 0 end as bit) as is_gearchiveerd,
        createdAt                   as aangemaakt_op,
        updatedAt                   as gewijzigd_op

    from bron

)

select * from definitief
