with

source as (

    select * from {{ source('ons_plan_2', 'care_order_products') }}

),

renamed as (

    select
        objectId as legitimatie_product_id,
        careOrderObjectId as legitimatie_id,
        beginDateClipped as startdatum_legitimatie_product,
        coalesce(endDateClipped, cast('9999-12-31' as date)) as einddatum_legitimatie_product,
        productObjectId as product_id,
        debtorObjectId as debiteur_id
    from source

)

select 
    legitimatie_product_id,
    legitimatie_id,
    startdatum_legitimatie_product,
    einddatum_legitimatie_product,
    product_id,
    debiteur_id
from renamed