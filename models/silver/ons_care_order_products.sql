-- =============================================================================
-- silver.ons_care_order_products
-- =============================================================================

{{ config(materialized='table', as_columnstore=false) }}

select
    [objectId]
    ,[careOrderObjectId]
    ,[id]
    ,[comments]
    ,[beginDate]
    ,[endDate]
    ,[beginDateClipped]
    ,[endDateClipped]
    ,[productObjectId]
    ,[awbzKlasseObjectId]
    ,[quantityInMinutes]
    ,[instellingsCode]
    ,[debtorObjectId]
    ,[unit]
    ,[frequency]
    ,[unitquantity]
    ,[createdAt]
    ,[updatedAt]
from {{ source('ons_plan_2', 'care_order_products') }}

