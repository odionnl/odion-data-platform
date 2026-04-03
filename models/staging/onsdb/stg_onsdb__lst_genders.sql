with bron as (

    select * from {{ source('ons_plan_2', 'lst_genders') }}

),

definitief as (

    select
        code            as geslacht_code,
        description     as geslacht

    from bron

)

select * from definitief
