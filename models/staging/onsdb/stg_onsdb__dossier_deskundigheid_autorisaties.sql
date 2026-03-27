with bron as (

    select * from {{ source('ons_plan_2', 'dossier_expertise_authorizations') }}

),

definitief as (

    select
        expertiseAuthorizableId     as episode_id,
        expertiseProfileId          as deskundigheid_id

    from bron

)

select * from definitief
