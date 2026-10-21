with bron as (

    select * from {{ source('ons_plan_2', 'cbsbj_hulpvormen') }}

),

definitief as (

    select
        code                    as hulpvorm_code,
        description             as hulpvorm_omschrijving

    from bron

)

select * from definitief
