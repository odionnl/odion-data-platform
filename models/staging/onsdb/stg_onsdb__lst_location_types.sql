with bron as (

    select * from {{ source('ons_plan_2', 'lst_location_types') }}

),

definitief as (

    select
        code            as locatietype_code,
        description     as locatietype

    from bron

)

select * from definitief
