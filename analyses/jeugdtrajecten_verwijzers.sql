USE Ons_Plan_2;

DECLARE @startdatum DATE = '2025-07-01';
DECLARE @einddatum  DATE = '2026-12-31';

SELECT
    j.clientObjectId,
    c.identificationNo AS clientnummer,
    h.description as hulpvorm,
    v.description as verwijzer,
    j.datumAanvang,
    j.datumBeeindiging
FROM cbsbj_jeugdhulps j
    LEFT JOIN cbsbj_hulpvormen h ON h.code = j.hulpvorm
    LEFT JOIN cbsbj_verwijzers v ON v.code = j.verwijzer
    LEFT JOIN clients c ON c.objectId=j.clientObjectId
WHERE j.datumAanvang <= @einddatum
    AND (j.datumBeeindiging >= @startdatum OR j.datumBeeindiging IS NULL)
    AND v.description = 'Geen verwijzer'