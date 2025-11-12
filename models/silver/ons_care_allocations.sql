-- =============================================================================
-- silver.ons_care_allocations
-- =============================================================================

{{ config(materialized='table') }}

SELECT
    clientObjectId,
    dateBegin,
    dateEnd
from {{ source('ons_plan_2', 'care_allocations') }}