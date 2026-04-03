with bron as (

    select * from {{ source('ons_plan_2', 'products') }}

),

definitief as (

    select
        objectId                as product_id,
        id                      as product_code,
        [description]           as product_omschrijving,
        beginDate               as startdatum,
        endDate                 as einddatum

    from bron

)

select * from definitief
