with bron as (

    select * from {{ source('ons_plan_2', 'expertise_groups') }}

),

definitief as (

    select
        objectId    as deskundigheidsgroep_id,
        name        as deskundigheidsgroep_naam

    from bron

)

select * from definitief
