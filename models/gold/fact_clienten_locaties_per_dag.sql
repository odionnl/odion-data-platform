-- =============================================================================
-- gold.fact_clienten_locaties_per_dag
-- =============================================================================
{{ config(materialized='view') }}

SELECT
    d.date_key,
    d.full_date AS peildatum,
    CASE 
        WHEN d.full_date > getdate() THEN 1
        ELSE 0
    END AS is_toekomst,
    la.clientObjectId,
    c.clientnummer,
    la.locationObjectId,
    la.locationType
FROM {{ source('silver', 'dim_date') }} AS d
LEFT JOIN {{ source('silver', 'ons_location_assignments') }} AS la
    ON la.beginDate <= d.full_date
   AND (la.endDate IS NULL OR la.endDate >= d.full_date)
INNER JOIN {{ ref('dim_clienten') }} AS c
    ON la.clientObjectId = c.clientObjectId;
