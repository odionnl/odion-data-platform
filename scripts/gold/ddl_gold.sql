USE OdionDataPlatform;
GO

-- =============================================================================
-- gold.dim_clients
-- =============================================================================
IF OBJECT_ID('gold.dim_clients', 'V') IS NOT NULL
    DROP VIEW gold.dim_clients;
GO

CREATE VIEW gold.dim_clients
AS

    SELECT
        ROW_NUMBER() OVER (ORDER BY c.objectId) AS client_key, -- Surrogate key
        c.objectId AS clientObjectId,
        c.identificationNo AS clientnummer,
        c.dateOfBirth AS geboortedatum,
        c.deathDate AS overlijdingsdatum
    FROM silver.ons_clients c;
GO

-- =============================================================================
-- gold.fact_clients_in_care_per_year
-- =============================================================================
IF OBJECT_ID('gold.fact_clients_in_care_per_year', 'V') IS NOT NULL
    DROP VIEW gold.fact_clients_in_care_per_year;
GO

CREATE VIEW gold.fact_clients_in_care_per_year
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY c.client_key, d.full_date) AS age_key, -- Surrogate key
        d.full_date AS datum,
        c.clientnummer,
        c.geboortedatum,
        c.overlijdingsdatum,
        DATEDIFF(YEAR, c.geboortedatum, d.full_date) AS leeftijd
    FROM silver.dim_date d
        LEFT JOIN silver.ons_care_allocations ca
        ON ca.dateBegin <= d.full_date
            AND (ca.dateEnd IS NULL OR ca.dateEnd >= d.full_date)
        LEFT JOIN gold.dim_clients c
        ON c.clientObjectId=ca.clientObjectId
    WHERE d.day=1
        AND d.month=1
        AND year <= YEAR(GETDATE())
GO



