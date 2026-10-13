with bron as (

    select * from {{ source('ons_plan_2', 'agbcodes') }}

),

definitief as (

    select
        objectId        as agbcode_id,
        code            as agbcode,
        [name]          as agbcode_naam,
        vatNumber       as btw_nummer,
        createdAt       as aangemaakt_op,
        updatedAt       as gewijzigd_op

    from bron

)

select * from definitief
