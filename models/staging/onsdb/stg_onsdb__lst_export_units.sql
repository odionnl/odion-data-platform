with bron as (

    select * from {{ source('ons_plan_2', 'lst_export_units') }}

),

definitief as (

    select
        code            as eenheid_code,
        description     as eenheid

    from bron

)

select * from definitief
