-- =============================================================================
-- silver.ons_locations
-- =============================================================================

{{ config(materialized='table') }}

select
    objectId,
    beginDate,
    endDate,
    [name],
    parentObjectId,
    materializedPath
from {{ source('ons_plan_2', 'locations') }};