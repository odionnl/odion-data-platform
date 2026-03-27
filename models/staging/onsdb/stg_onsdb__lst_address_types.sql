with bron as (

    select * from {{ source('ons_plan_2', 'lst_address_types') }}

),

definitief as (

    select
        code            as adrestype_code,
        description     as adrestype

    from bron

)

select * from definitief
