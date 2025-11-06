-- =============================================================================
-- gold.fact_clienten_overleden
-- =============================================================================

{{ config(materialized='view') }}

-- TODO: also add location information (names and niveau) at death date; not sure how to solve in PowerBI?
SELECT
    c.clientObjectId,
    c.clientnummer,
    c.geboortedatum,
    c.overlijdensdatum,
    d_overlijden.date_key AS date_key_overlijden,
    DATEDIFF(YEAR, c.geboortedatum, c.overlijdensdatum)
    - CASE
        WHEN DATEADD(
                YEAR,
                DATEDIFF(YEAR, c.geboortedatum, c.overlijdensdatum),
                c.geboortedatum
            ) > c.overlijdensdatum
        THEN 1 ELSE 0
    END AS leeftijd_bij_overlijden,
    cl.locationObjectId AS hoofdlocatie_bij_overlijden
FROM {{ ref('dim_clienten') }} AS c
    -- only clients with a death date
    LEFT JOIN {{ source('silver', 'dim_date') }} AS d_overlijden
        ON d_overlijden.full_date = CAST(c.overlijdensdatum AS date)
    LEFT JOIN {{ ref('fact_clienten_locaties_per_dag') }} AS cl 
        ON c.clientObjectId = cl.clientObjectId 
        AND cl.peildatum = CAST(DATEADD(DAY, -1, c.overlijdensdatum) AS date)
        AND locationType='MAIN'
WHERE c.overlijdensdatum IS NOT NULL
    -- ensure the client had an active care allocation on the death date,
    -- without duplicating rows if multiple allocations overlap
    AND EXISTS (
    SELECT 1
    FROM {{ source('silver', 'ons_care_allocations') }} AS ca
    WHERE ca.clientObjectId = c.clientObjectId
        AND ca.dateBegin <= c.overlijdensdatum
        AND (ca.dateEnd  >= c.overlijdensdatum OR ca.dateEnd IS NULL)
    );

