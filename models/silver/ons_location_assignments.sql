-- =============================================================================
-- silver.ons_location_assignments
-- =============================================================================

{{ config(materialized='table') }}

SELECT
    clientObjectId,
    locationObjectId,
    beginDate,
    endDate,
    locationType
from {{ source('ons_plan_2', 'location_assignments') }}