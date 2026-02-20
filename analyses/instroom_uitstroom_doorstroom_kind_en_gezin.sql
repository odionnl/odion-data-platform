/*==============================================================
In- en uitstroom van cliënten per jaar (2023 t/m 2025).

Kolommen:
- totaal_instroom:      startdatum_zorg valt binnen het jaar (alle clusters)
- totaal_uitstroom:     einddatum_zorg valt binnen het jaar (alle clusters)
- instroom_kind_gezin:  locatietoewijzing in K&G start binnen het jaar,
                        maar client was NIET al in K&G op 1 januari (interne overplaatsingen uitgesloten)
- uitstroom_kind_gezin: einddatum_zorg valt binnen het jaar én client was in K&G
- doorstroom:           einddatum_locatie in K&G valt binnen het jaar,
                        maar einddatum_zorg valt er NIET in (zorg loopt door),
                        én client is NIET nog actief in K&G op 31 december (interne overplaatsingen uitgesloten)
- beginstand:           actief in K&G op 1 januari van het jaar
- eindstand:            actief in K&G op 31 december van het jaar

Consistentiecheck: beginstand + instroom - uitstroom - doorstroom = eindstand

Opzet: tijdelijke tabellen zodat elke stap éénmalig wordt berekend en
hergebruikt, in plaats van meerdere keren als CTE opnieuw te worden uitgewerkt.
==============================================================*/

DROP TABLE IF EXISTS #totaal;
DROP TABLE IF EXISTS #kg_actief;
DROP TABLE IF EXISTS #kg_actief_jan1;
DROP TABLE IF EXISTS #kg_instroom;
DROP TABLE IF EXISTS #kg_locatie_einde;
DROP TABLE IF EXISTS #zorg_einde;
DROP TABLE IF EXISTS #kg_uitstroom;
DROP TABLE IF EXISTS #beginstand;
DROP TABLE IF EXISTS #eindstand;
DROP TABLE IF EXISTS #doorstroom;

------------------------------------------------------------------------
-- Stap 1: Totaal in- en uitstroom op basis van zorgopname (alle clusters)
------------------------------------------------------------------------
SELECT
    j.jaar,
    ca.client_id,
    CASE WHEN ca.startdatum_zorg BETWEEN j.jaar_start AND j.jaar_eind THEN 1 ELSE 0 END AS is_instroom,
    CASE WHEN ca.einddatum_zorg  BETWEEN j.jaar_start AND j.jaar_eind THEN 1 ELSE 0 END AS is_uitstroom
INTO #totaal
FROM [OdionDataPlatform].[odp_intermediate].[int_care_allocations]  ca
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
WHERE ca.startdatum_zorg BETWEEN j.jaar_start AND j.jaar_eind
   OR ca.einddatum_zorg  BETWEEN j.jaar_start AND j.jaar_eind;

------------------------------------------------------------------------
-- Stap 2: K&G actief in het jaar (overlapfilter — grondslag voor uitstroom K&G)
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #kg_actief
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
WHERE la.startdatum_locatie <= j.jaar_eind
  AND la.einddatum_locatie  >= j.jaar_start
  AND lh.cluster             = N'Kind en gezin';

------------------------------------------------------------------------
-- Stap 3: K&G actief op 1 januari (filter instroom: interne overplaatsingen uitsluiten)
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #kg_actief_jan1
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
WHERE la.startdatum_locatie <  j.jaar_start   -- gestart vóór 1 jan
  AND la.einddatum_locatie  >= j.jaar_start   -- nog actief op 1 jan
  AND lh.cluster             = N'Kind en gezin';

------------------------------------------------------------------------
-- Stap 4: K&G instroom (locatietoewijzing start in jaar, exclusief al actief op 1 jan)
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #kg_instroom
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
LEFT JOIN #kg_actief_jan1  jan1
    ON  jan1.client_id = la.client_id
    AND jan1.jaar      = j.jaar
WHERE la.startdatum_locatie BETWEEN j.jaar_start AND j.jaar_eind
  AND lh.cluster      = N'Kind en gezin'
  AND jan1.client_id IS NULL;   -- was NIET al in K&G op 1 jan

------------------------------------------------------------------------
-- Stap 5: K&G locatietoewijzingen die eindigen binnen het jaar (basis voor doorstroom)
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #kg_locatie_einde
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
WHERE la.einddatum_locatie BETWEEN j.jaar_start AND j.jaar_eind
  AND lh.cluster = N'Kind en gezin';

------------------------------------------------------------------------
-- Stap 6: Zorgopnames die eindigen binnen het jaar (alle clusters)
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, ca.client_id
INTO #zorg_einde
FROM [OdionDataPlatform].[odp_intermediate].[int_care_allocations]  ca
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
WHERE ca.einddatum_zorg BETWEEN j.jaar_start AND j.jaar_eind;

------------------------------------------------------------------------
-- Stap 7: Uitstroom K&G — zorgopname eindigt in jaar én client was in K&G
------------------------------------------------------------------------
SELECT DISTINCT ze.jaar, ze.client_id
INTO #kg_uitstroom
FROM #zorg_einde  ze
INNER JOIN #kg_actief  ka
    ON  ka.client_id = ze.client_id
    AND ka.jaar      = ze.jaar;

------------------------------------------------------------------------
-- Stap 8: Beginstand — actief in K&G op 1 januari van het jaar
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #beginstand
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE))
) AS j(jaar, jan1)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_care_allocations]  ca
    ON  ca.client_id       = la.client_id
    AND ca.startdatum_zorg <  j.jan1
    AND (ca.einddatum_zorg IS NULL OR ca.einddatum_zorg >= j.jan1)
WHERE la.startdatum_locatie <  j.jan1
  AND la.einddatum_locatie  >= j.jan1
  AND lh.cluster             = N'Kind en gezin';

------------------------------------------------------------------------
-- Stap 9: Eindstand — actief in K&G op 31 december van het jaar
------------------------------------------------------------------------
SELECT DISTINCT j.jaar, la.client_id
INTO #eindstand
FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
CROSS JOIN (VALUES
    (2023, CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)),
    (2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)),
    (2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE))
) AS j(jaar, jaar_start, jaar_eind)
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
    ON lh.locatie_id = la.locatie_id
INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_care_allocations]  ca
    ON  ca.client_id       = la.client_id
    AND ca.startdatum_zorg <= j.jaar_eind
    AND (ca.einddatum_zorg IS NULL OR ca.einddatum_zorg > j.jaar_eind)
WHERE la.startdatum_locatie <= j.jaar_eind
  AND la.einddatum_locatie  >  j.jaar_eind
  AND lh.cluster             = N'Kind en gezin';

------------------------------------------------------------------------
-- Stap 10: Doorstroom — K&G locatie eindigt in jaar, zorgopname NIET,
--          én client is NIET nog actief in K&G op 31 dec (interne overplaatsingen uitsluiten)
------------------------------------------------------------------------
SELECT DISTINCT kle.jaar, kle.client_id
INTO #doorstroom
FROM #kg_locatie_einde  kle
LEFT JOIN #zorg_einde  ze
    ON  ze.client_id = kle.client_id
    AND ze.jaar      = kle.jaar
LEFT JOIN #eindstand  es
    ON  es.client_id = kle.client_id
    AND es.jaar      = kle.jaar
WHERE ze.client_id IS NULL   -- zorgopname eindigt NIET in dit jaar
  AND es.client_id IS NULL;  -- NIET nog actief in K&G op 31 dec (anders interne overplaatsing)

------------------------------------------------------------------------
-- Eindresultaat
------------------------------------------------------------------------
SELECT
    j.jaar,
    t.totaal_instroom,
    t.totaal_uitstroom,
    bs.beginstand_kind_gezin,
    ki.instroom_kind_gezin,
    ku.uitstroom_kind_gezin,
    ds.doorstroom_kind_gezin,
    es.eindstand_kind_gezin

FROM (VALUES (2023), (2024), (2025))  AS j(jaar)

LEFT JOIN (
    SELECT
        jaar,
        COUNT(DISTINCT CASE WHEN is_instroom  = 1 THEN client_id END)  AS totaal_instroom,
        COUNT(DISTINCT CASE WHEN is_uitstroom = 1 THEN client_id END)  AS totaal_uitstroom
    FROM #totaal
    GROUP BY jaar
)  t  ON t.jaar = j.jaar

LEFT JOIN (
    SELECT jaar, COUNT(DISTINCT client_id) AS beginstand_kind_gezin
    FROM #beginstand
    GROUP BY jaar
)  bs  ON bs.jaar = j.jaar

LEFT JOIN (
    SELECT jaar, COUNT(DISTINCT client_id) AS instroom_kind_gezin
    FROM #kg_instroom
    GROUP BY jaar
)  ki  ON ki.jaar = j.jaar

LEFT JOIN (
    SELECT jaar, COUNT(DISTINCT client_id) AS uitstroom_kind_gezin
    FROM #kg_uitstroom
    GROUP BY jaar
)  ku  ON ku.jaar = j.jaar

LEFT JOIN (
    SELECT jaar, COUNT(DISTINCT client_id) AS doorstroom_kind_gezin
    FROM #doorstroom
    GROUP BY jaar
)  ds  ON ds.jaar = j.jaar

LEFT JOIN (
    SELECT jaar, COUNT(DISTINCT client_id) AS eindstand_kind_gezin
    FROM #eindstand
    GROUP BY jaar
)  es  ON es.jaar = j.jaar

ORDER BY j.jaar;
