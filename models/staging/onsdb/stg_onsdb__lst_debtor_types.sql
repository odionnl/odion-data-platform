with bron as (

    select * from {{ source('ons_plan_2', 'lst_debtor_types') }}

),

definitief as (

    select
        code            as debiteur_type_code,
        description     as debiteur_type

    from bron

)

select * from definitief
