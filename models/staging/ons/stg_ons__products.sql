with source as (

    select * 
    from {{ source('ons_plan_2', 'products') }}

),

renamed as (

    select
        objectId            as product_id,
        id                  as product_code,
        description         as product_omschrijving,
        beginDate           as startdatum_product,
        endDate             as einddatum_product,
        financeTypeObjectId as financieringstype_id,
        parentObjectId      as ouder_product_id
    from source

)

select
    product_id,
    product_code,
    product_omschrijving,
    startdatum_product,
    einddatum_product,
    financieringstype_id,
    ouder_product_id
from renamed;
