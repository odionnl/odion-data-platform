with bron as (

    select * from {{ source('ons_plan_2', 'dossier_expertise_group_authorizations') }}

),

definitief as (

    select
        expertiseGroupAuthorizableId    as episode_id,
        expertiseGroupId                as deskundigheidsgroep_id

    from bron

)

select * from definitief
