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

-- =============================================================================
-- gold.fact_clienten_overleden
-- =============================================================================


CREATE OR ALTER VIEW gold.fact_clienten_overleden
AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY overlijdensdatum, c.clientObjectId) AS client_overleden_key,
        c.client_key,
        c.clientnummer,
        c.geboortedatum,
        c.overlijdensdatum,
        DATEDIFF(YEAR, c.geboortedatum, c.overlijdensdatum)
        - CASE
            WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.geboortedatum, c.overlijdensdatum), c.geboortedatum) > c.overlijdensdatum
            THEN 1 ELSE 0
        END AS leeftijd_bij_overlijden
    FROM gold.dim_clienten c
        INNER JOIN silver.ons_care_allocations ca
        ON ca.clientObjectId = c.clientObjectId
            AND ca.dateBegin <= c.overlijdensdatum
            AND (ca.dateEnd >= c.overlijdensdatum OR ca.dateEnd IS NULL)
    WHERE c.overlijdensdatum IS NOT NULL;
GO


-- =============================================================================
-- gold.dim_locaties
-- =============================================================================

CREATE OR ALTER VIEW gold.dim_locaties
AS
    WITH
        H
        AS
        (
            -- Anchor: rootlocaties (niveau 1)
                            SELECT
                    l.objectId, -- Uniek ID van de locatie
                    l.parentObjectId, -- Bovenliggende locatie
                    l.beginDate      AS startdatum, -- Startdatum van locatie
                    l.endDate        AS einddatum, -- Einddatum van locatie
                    CAST(l.name AS nvarchar(255)) AS locatienaam, -- Naam huidige locatie
                    CAST(l.name AS nvarchar(255)) AS niveau1,
                    CAST(NULL AS nvarchar(255)) AS niveau2,
                    CAST(NULL AS nvarchar(255)) AS niveau3,
                    CAST(NULL AS nvarchar(255)) AS niveau4,
                    CAST(NULL AS nvarchar(255)) AS niveau5,
                    CAST(NULL AS nvarchar(255)) AS niveau6,
                    1 AS niveau, -- Huidig niveau (root = 1)
                    CAST('.' + CAST(l.objectId AS nvarchar(50)) + '.' AS nvarchar(1000)) AS pad
                FROM silver.ons_locations AS l
                WHERE l.parentObjectId IS NULL

            UNION ALL

                -- Recursief deel
                SELECT
                    c.objectId,
                    c.parentObjectId,
                    c.beginDate,
                    c.endDate,
                    CAST(c.name AS nvarchar(255)) AS locatienaam, -- Naam huidige locatie
                    h.niveau1,
                    CASE WHEN h.niveau = 1 THEN c.name ELSE h.niveau2 END AS niveau2,
                    CASE WHEN h.niveau = 2 THEN c.name ELSE h.niveau3 END AS niveau3,
                    CASE WHEN h.niveau = 3 THEN c.name ELSE h.niveau4 END AS niveau4,
                    CASE WHEN h.niveau = 4 THEN c.name ELSE h.niveau5 END AS niveau5,
                    CASE WHEN h.niveau = 5 THEN c.name ELSE h.niveau6 END AS niveau6,
                    h.niveau + 1 AS niveau,
                    CAST(h.pad + CAST(c.objectId AS nvarchar(50)) + '.' AS nvarchar(1000)) AS pad
                FROM silver.ons_locations AS c
                    INNER JOIN H
                    ON c.parentObjectId = H.objectId
                WHERE h.niveau < 6
        )
    SELECT
        ROW_NUMBER() OVER (ORDER BY H.objectId) AS locatie_key, -- Surrogate key
        H.objectId           AS locationObjectId,
        H.locatienaam, -- Huidige locatie
        H.startdatum,
        H.einddatum,
        H.pad                AS materializedPath, -- Genormaliseerd pad
        H.niveau - 1         AS locatie_niveau, -- Root = 0
        CASE 
        WHEN (H.startdatum IS NULL OR H.startdatum <= CAST(GETDATE() AS date))
            AND (H.einddatum IS NULL OR H.einddatum >= CAST(GETDATE() AS date))
            THEN 1 
        ELSE 0 
    END AS is_actueel, -- Actief vandaag (1/0)
        H.niveau1,
        H.niveau2,
        H.niveau3,
        H.niveau4,
        H.niveau5,
        H.niveau6,
        CASE
        WHEN H.niveau3 LIKE '%Wonen%'
            OR H.niveau3 LIKE '%Dagbesteding%'
            OR H.niveau3 LIKE '%Ambulant%'
            OR H.niveau3 LIKE '%Kind en gezin%'
            OR H.niveau3 LIKE '%Logeren%'
        THEN TRIM(SUBSTRING(H.niveau3, 5, LEN(H.niveau3)))
        ELSE 'Overig'
    END AS cluster
    FROM H
    WHERE H.niveau >= 2;
GO




-- =============================================================================
-- gold.fact_clienten_locaties_per_dag
-- =============================================================================


-- SELECT TOP(1000)
--     d.full_date AS peildatum,
--     CASE 
--         WHEN d.full_date > GETDATE() THEN 1
--         ELSE 0
--     END AS is_toekomst,
--     c.clientnummer,
--     l.name,
--     l.materializedPath,
--     la.locationType
-- FROM silver.dim_date d
--     LEFT JOIN silver.ons_location_assignments la
--     ON la.beginDate <= d.full_date
--         AND (la.endDate IS NULL OR la.endDate >= d.full_date)
--     INNER JOIN gold.dim_clienten c ON la.clientObjectId=c.clientObjectId
--     LEFT JOIN silver.ons_locations l ON l.objectId=la.locationObjectId
-- WHERE d.full_date = CAST(GETDATE() AS date);