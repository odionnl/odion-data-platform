with bron as (

    select * from {{ source('ons_plan_2', 'expertise_group_expertise_profiles') }}

),

definitief as (

    select
        expertiseGroupObjectId   as deskundigheidsgroep_id,
        expertiseProfileObjectId as deskundigheid_id

    from bron

)

select * from definitief
