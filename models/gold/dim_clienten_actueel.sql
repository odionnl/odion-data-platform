-- =============================================================================
-- gold.dim_clienten_actueel
-- =============================================================================
{{ config(
    materialized='view'
) }}

SELECT
    c.*
FROM {{ ref('dim_clienten') }} AS c
INNER JOIN {{ source('silver', 'ons_care_allocations') }} AS ca
    ON c.clientObjectId = ca.clientObjectId
    AND ca.dateBegin <= getdate()
    AND (ca.dateEnd IS NULL OR ca.dateEnd >= getdate());
