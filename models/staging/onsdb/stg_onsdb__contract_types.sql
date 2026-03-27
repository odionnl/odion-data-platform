with bron as (

    select * from {{ source('ons_plan_2', 'contract_types') }}

),

definitief as (

    select
        objectId                as contracttype_id,
        name                    as contracttype_naam,
        code                    as contracttype_code

    from bron

)

select * from definitief
