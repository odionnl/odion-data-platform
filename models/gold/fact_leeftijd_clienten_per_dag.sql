-- =============================================================================
-- gold.fact_leeftijd_clienten_per_dag
-- =============================================================================
{{ config(materialized='view') }}

SELECT
    d.date_key,
    d.full_date AS peildatum,
    c.clientObjectId,
    c.clientnummer,
    c.geboortedatum,
    c.overlijdensdatum,
    a.leeftijd,
    CASE
        WHEN a.leeftijd < 18 THEN '<18'
        WHEN a.leeftijd BETWEEN 18 AND 49 THEN '18-49'
        WHEN a.leeftijd >= 50 THEN '50+'
        ELSE 'Onbekend'
    END AS leeftijdsgroep,
    cl.locationObjectId AS hoofdlocatie_peildatum
FROM {{ source('silver', 'dim_date') }} AS d
    LEFT JOIN {{ source('silver', 'ons_care_allocations') }} AS ca
        ON ca.dateBegin <= d.full_date
        AND (ca.dateEnd IS NULL OR ca.dateEnd >= d.full_date)
    LEFT JOIN {{ ref('dim_clienten') }} AS c
        ON c.clientObjectId = ca.clientObjectId
    LEFT JOIN {{ ref('fact_clienten_locaties_per_dag') }} AS cl 
        ON c.clientObjectId = cl.clientObjectId 
        AND cl.peildatum = d.full_date
        AND locationType='MAIN'

CROSS APPLY (
    -- Leeftijd berekenen, rekening houdend met maand en dag
    SELECT
        DATEDIFF(YEAR, c.geboortedatum, d.full_date)
        - CASE
            WHEN (MONTH(d.full_date) * 100 + DAY(d.full_date))
               < (MONTH(c.geboortedatum) * 100 + DAY(c.geboortedatum))
              THEN 1 ELSE 0
          END
) AS a(leeftijd)
WHERE d.full_date <= getdate()
    AND c.geboortedatum IS NOT NULL
    AND d.full_date >= c.geboortedatum
    -- Geen leeftijd berekenen na overlijden
    AND (c.overlijdensdatum IS NULL OR d.full_date <= c.overlijdensdatum);
