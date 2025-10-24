USE OdionDataPlatform;
GO

-- =============================================================================
-- gold.dim_clienten
-- =============================================================================

CREATE OR ALTER VIEW gold.dim_clienten
AS

    SELECT
        ROW_NUMBER() OVER (ORDER BY c.objectId) AS client_key, -- Surrogate key
        c.objectId AS clientObjectId,
        c.identificationNo AS clientnummer,
        c.dateOfBirth AS geboortedatum,
        c.deathDate AS overlijdensdatum
    FROM silver.ons_clients c;
GO

-- =============================================================================
-- gold.fact_leeftijd_clienten_per_dag
-- =============================================================================

CREATE OR ALTER VIEW gold.fact_leeftijd_clienten_per_dag
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.client_key, d.full_date) AS fact_leeftijd_client_dag_key, -- Surrogate key
        d.full_date AS peildatum,
        CASE 
            WHEN d.full_date > GETDATE() THEN 1
            ELSE 0
        END AS is_toekomst,
        c.clientnummer,
        c.geboortedatum,
        c.overlijdensdatum,
        a.leeftijd,
        CASE
            WHEN a.leeftijd < 18 THEN '<18'
            WHEN a.leeftijd BETWEEN 18 AND 49 THEN '18-49'
            WHEN a.leeftijd >= 50 THEN '50+'
            ELSE 'Onbekend'
        END AS leeftijdsgroep
    FROM silver.dim_date d
        LEFT JOIN silver.ons_care_allocations ca
        ON ca.dateBegin <= d.full_date
            AND (ca.dateEnd IS NULL OR ca.dateEnd >= d.full_date)
        LEFT JOIN gold.dim_clienten c
        ON c.clientObjectId = ca.clientObjectId
CROSS APPLY (
    -- Leeftijd berekenen, rekening houdend met maand en dag
    SELECT DATEDIFF(YEAR, c.geboortedatum, d.full_date)
           - CASE
               WHEN (MONTH(d.full_date) * 100 + DAY(d.full_date))
                    < (MONTH(c.geboortedatum) * 100 + DAY(c.geboortedatum))
                 THEN 1 ELSE 0
             END
) AS a(leeftijd)
    WHERE c.geboortedatum IS NOT NULL
        AND d.full_date >= c.geboortedatum
        -- Geen leeftijd berekenen na overlijden
        AND (c.overlijdensdatum IS NULL OR d.full_date <= c.overlijdensdatum);
GO

-- =============================================================================
-- gold.fact_leeftijd_clienten_per_jaar
-- =============================================================================


CREATE OR ALTER VIEW gold.fact_leeftijd_clienten_per_jaar
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY fact_leeftijd_client_dag_key, peildatum) AS fact_leeftijd_client_jaar_key,
        *
    FROM gold.fact_leeftijd_clienten_per_dag
    WHERE DATEPART(DAY,   peildatum) = 1
        AND DATEPART(MONTH, peildatum) = 1;
GO

-- =============================================================================
-- gold.fact_leeftijd_clienten_per_maand
-- =============================================================================


CREATE OR ALTER VIEW gold.fact_leeftijd_clienten_per_maand
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY fact_leeftijd_client_dag_key, peildatum) AS fact_leeftijd_client_jaar_key,
        *
    FROM gold.fact_leeftijd_clienten_per_dag
    WHERE DATEPART(DAY,   peildatum) = 1;
GO