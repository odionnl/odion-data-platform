with bron as (

    select * from {{ source('ons_plan_2', 'lst_invoice_types') }}

),

definitief as (

    select
        code            as factuur_type_code,
        description     as factuur_type

    from bron

)

select * from definitief
