-- =============================================================================
-- gold.dim_locaties
-- =============================================================================
{{ config(materialized='view') }}

WITH H AS (
    -- Anchor: rootlocaties (niveau 1)
    SELECT
        l.objectId,                               -- Uniek ID van de locatie
        l.parentObjectId,                         -- Bovenliggende locatie
        l.beginDate      AS startdatum,           -- Startdatum van locatie
        l.endDate        AS einddatum,            -- Einddatum van locatie
        CAST(l.name AS nvarchar(255)) AS locatienaam, -- Naam huidige locatie
        CAST(l.name AS nvarchar(255)) AS niveau1,
        CAST(NULL AS nvarchar(255)) AS niveau2,
        CAST(NULL AS nvarchar(255)) AS niveau3,
        CAST(NULL AS nvarchar(255)) AS niveau4,
        CAST(NULL AS nvarchar(255)) AS niveau5,
        CAST(NULL AS nvarchar(255)) AS niveau6,
        1 AS niveau,                              -- Huidig niveau (root = 1)
        CAST('.' + CAST(l.objectId AS nvarchar(50)) + '.' AS nvarchar(1000)) AS pad
    FROM {{ source('silver', 'ons_locations') }} AS l
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
    FROM {{ source('silver', 'ons_locations') }} AS c
    INNER JOIN H
        ON c.parentObjectId = H.objectId
    WHERE h.niveau < 6
)

SELECT
    H.objectId     AS locationObjectId,
    H.locatienaam,                             -- Huidige locatie
    H.startdatum,
    H.einddatum,
    H.pad          AS materializedPath,        -- Genormaliseerd pad
    H.niveau - 1   AS locatie_niveau,          -- Root = 0
    CASE 
        WHEN (H.startdatum IS NULL OR H.startdatum <= CAST(getdate() AS date))
         AND (H.einddatum IS NULL OR H.einddatum >= CAST(getdate() AS date))
        THEN 1 ELSE 0
    END AS is_actueel,                         -- Actief vandaag (1/0)
    H.niveau1,
    H.niveau2,
    H.niveau3,
    H.niveau4,
    H.niveau5,
    H.niveau6,
    CASE
        -- Externe aanbieder
        WHEN LOWER(ISNULL(H.locatienaam, '')) LIKE '%externe aanbieder%'
          OR ISNULL(H.niveau2,'') LIKE '%Externe aanbieder%'
          THEN N'Externe aanbieder'

        -- Aanmeldingen
        WHEN ISNULL(H.niveau2,'') LIKE '%Aanmeldingen%'
          THEN N'Aanmeldingen'

        -- Wachtlijsten
        WHEN LOWER(ISNULL(H.locatienaam, '')) LIKE '%wachtlijsten%'
          OR ISNULL(H.niveau2,'') LIKE '%Wachtlijsten%'
          THEN N'Wachtlijsten'

        -- Archief
        WHEN ISNULL(H.niveau2,'') LIKE '%Archief%'
          THEN N'Archief'

        -- Overige
        WHEN ISNULL(H.niveau2,'') LIKE '%Overige%'
          THEN N'Overige'

        -- Ambulant
        WHEN LOWER(ISNULL(H.locatienaam, '')) LIKE '%in de wijk%'
          OR ISNULL(H.niveau3,'') LIKE '%Ambulant%'
          THEN N'Ambulant'

        -- Dagbesteding
        WHEN LOWER(ISNULL(H.locatienaam, '')) LIKE '%dagbesteding%'
          OR ISNULL(H.niveau3,'') LIKE '%Dagbesteding%'
          THEN N'Dagbesteding'

        -- Logeren
        WHEN LOWER(ISNULL(H.locatienaam, '')) LIKE '%logeren%'
          OR ISNULL(H.niveau3,'') LIKE '%Logeren%'
          THEN N'Logeren'

        -- Wonen
        WHEN ISNULL(H.niveau3,'') LIKE '%Wonen%'
          THEN N'Wonen'

        -- Behandeling
        WHEN ISNULL(H.niveau3,'') LIKE '%Behandeling%'
          THEN N'Behandeling'

        -- Kind en gezin
        WHEN ISNULL(H.niveau3,'') LIKE '%Kind en gezin%'
          THEN N'Kind en gezin'

        -- Multidisciplinair Team
        WHEN ISNULL(H.niveau3,'') LIKE '%Multidisciplinair Team%'
          THEN N'Multidisciplinair Team'

        ELSE N'Overige'
    END AS cluster
FROM H
WHERE H.niveau >= 2;
