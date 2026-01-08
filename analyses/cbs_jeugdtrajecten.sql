USE Ons_Plan_2;

DECLARE @jaar INT = 2025;
DECLARE @startdatum DATE = CAST(CONCAT(@jaar, '-01-01') AS DATE);
DECLARE @einddatum DATE  = CAST(CONCAT(@jaar, '-06-30') AS DATE);

SELECT DISTINCT
    l.clientno,
    l.clientObjectId,
    l.client,
    CONVERT(varchar, l.geboren, 105) AS geboortedatum,
    -- ISNULL(CONVERT(varchar, l.tot, 105), '') AS einddatum_legitimatie,
    ISNULL(CONVERT(varchar, ca.dateBegin, 105), '') AS startdatum_in_zorg,
    ISNULL(CONVERT(varchar, ca.dateEnd, 105), '') AS einddatum_in_zorg,
    l.legitimatie,

    -- Samengevoegde producten
    ISNULL(producten.aantal_producten, 0) AS aantal_producten,
    ISNULL(producten.productenlijst, '') AS producten,

    -- Samengevoegde jeugdzorgtrajecten
    ISNULL(trajecten.aantal_actieve_jeugdtrajecten, 0) AS aantal_actieve_jeugdtrajecten,
    ISNULL(trajecten.actieve_jeugdtrajecten, '') AS actieve_jeugdtrajecten


FROM legitimaties AS l

    -- Zorgtoewijzing in periode
    INNER JOIN care_allocations AS ca
    ON ca.clientObjectId = l.clientobjectid
        AND ca.dateBegin <= @einddatum
        AND (ca.dateEnd IS NULL OR ca.dateEnd > @startdatum)

    -- Producten (aggregatie)
    LEFT JOIN (
    SELECT
        clientObjectId,
        STRING_AGG(product, ' | ') AS productenlijst,
        COUNT(*) AS aantal_producten
    FROM legitimaties
    WHERE product IS NOT NULL
        AND van <= @einddatum
        AND (tot IS NULL OR tot > @startdatum)
        AND [product van] <= @einddatum
        AND ([product tot] IS NULL OR [product tot] > @startdatum)
    GROUP BY clientObjectId
) AS producten ON producten.clientObjectId = l.clientObjectId

    -- Jeugdtrajecten (aggregatie)
    LEFT JOIN (
    SELECT
        clientObjectId,
        STRING_AGG(description, ' | ') AS actieve_jeugdtrajecten,
        COUNT(*) AS aantal_actieve_jeugdtrajecten
    FROM (
        SELECT
            j.clientObjectId,
            h.description
        FROM cbsbj_jeugdhulps j
            LEFT JOIN cbsbj_hulpvormen h ON h.code = j.hulpvorm
        WHERE j.datumAanvang <= @einddatum
            AND (j.datumBeeindiging >= @startdatum OR j.datumBeeindiging IS NULL)
    ) AS x
    GROUP BY clientObjectId
) AS trajecten ON trajecten.clientObjectId = l.clientObjectId


-- Filter op geldige legitimatie + productperiode
WHERE l.legitimatie LIKE '%JW%'
    AND l.van <= @einddatum
    AND (l.tot IS NULL OR l.tot > @startdatum)
    AND l.[product van] <= @einddatum
    AND (l.[product tot] IS NULL OR l.[product tot] > @startdatum)

ORDER BY l.client;
