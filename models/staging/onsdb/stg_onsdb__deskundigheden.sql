with bron as (

    select * from {{ source('ons_plan_2', 'expertise_profiles') }}

),

definitief as (

    select
        objectId    as deskundigheid_id,
        code        as deskundigheid_code,
        description as deskundigheid_naam,
        visible     as is_zichtbaar

    from bron

)

select * from definitief
