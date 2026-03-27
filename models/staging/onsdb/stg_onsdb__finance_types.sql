with bron as (

    select * from {{ source('ons_plan_2', 'finance_types') }}

),

definitief as (

    select
        objectId            as financieringstype_id,
        id                  as financieringstype_code,
        description         as financieringstype_naam,
        category            as categorie,
        financeTypeGroup    as financieringsgroep,
        beginDate           as startdatum,
        endDate             as einddatum,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op

    from bron

)

select * from definitief
