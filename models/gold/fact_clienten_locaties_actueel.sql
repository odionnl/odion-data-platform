-- =============================================================================
-- gold.fact_clienten_locaties_actueel
-- =============================================================================
{{ config(materialized='view') }}


SELECT
    la.clientObjectId,
    c.clientnummer,
    la.locationObjectId,
    la.locationType
FROM {{ source('silver', 'ons_location_assignments') }} AS la
INNER JOIN {{ ref('dim_clienten') }} AS c
    ON la.clientObjectId = c.clientObjectId
WHERE la.beginDate <= getdate()
  AND (la.endDate IS NULL OR la.endDate >= getdate());
