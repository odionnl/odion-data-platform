USE Ons_Plan_2;

SELECT
    cr.clientObjectId as client_id,
    c.identificationNo AS clientnummer,
    cr.objectId AS rapportage_id,
    cr.reportingDate as rapportagedatum,
    ep.description AS deskundigheid,
    eg.name AS deskundigheidsgroep,
    cr.hidden AS afgeschermd,
    CASE
        WHEN eg.name IS NOT NULL AND ep.description IS NOT NULL THEN 'beiden'
        WHEN eg.name IS NULL AND ep.description IS NOT NULL THEN 'deskundigheid'
        WHEN eg.name IS NOT NULL AND ep.description IS NULL THEN 'deskundigheidsgroep'
        ELSE 'geen'
    END AS 'afgeschermd_voor'
FROM careplan_reports cr
    LEFT JOIN careplan_report_action_rights crar
    ON cr.objectId = crar.careplanReportObjectId
    JOIN clients c
    ON c.objectId = cr.clientObjectId
    LEFT JOIN expertise_profiles ep
    ON crar.educationObjectId = ep.objectId
    LEFT JOIN expertise_groups eg
    ON eg.objectId = crar.expertiseGroupObjectId
WHERE 
    (ep.objectId IS NOT NULL OR eg.objectId IS NOT NULL)
    AND cr.reportingDate >= '2025-01-01' --yyyymmdd