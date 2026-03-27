with bron as (

    select * from {{ source('ons_plan_2', 'tags') }}

),

definitief as (

    select
        objectId    as tag_id,
        name        as label_naam

    from bron

)

select * from definitief
