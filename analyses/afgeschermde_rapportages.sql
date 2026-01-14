USE Ons_Plan_2;

SELECT
    cr.clientObjectId AS client_id,
    c.identificationNo AS clientnummer,
    cr.objectId AS rapportage_id,
    cr.reportingDate AS rapportagedatum,
    cr.hidden AS verborgen,

    /* Only show when type = 'Zichtbaar voor' */
    CASE
        WHEN crart.description = 'Zichtbaar voor'
        THEN ep.description
        ELSE NULL
    END AS deskundigheid,

    CASE
        WHEN crart.description = 'Zichtbaar voor'
        THEN eg.name
        ELSE NULL
    END AS deskundigheidsgroep,

    /* Afgeschermd only relevant for 'Zichtbaar voor' */
    CASE
        WHEN crart.description = 'Zichtbaar voor'
        AND (eg.name IS NOT NULL OR ep.description IS NOT NULL)
        THEN 1
        ELSE 0
    END AS afgeschermd,

    CASE
        WHEN crart.description <> 'Zichtbaar voor' THEN 'geen'
        WHEN eg.name IS NOT NULL AND ep.description IS NOT NULL THEN 'beiden'
        WHEN eg.name IS NULL AND ep.description IS NOT NULL THEN 'deskundigheid'
        WHEN eg.name IS NOT NULL AND ep.description IS NULL THEN 'deskundigheidsgroep'
        ELSE 'geen'
    END AS afgeschermd_voor

FROM careplan_reports cr
    LEFT JOIN careplan_report_action_rights crar
    ON cr.objectId = crar.careplanReportObjectId
    LEFT JOIN lst_careplan_report_action_right_types crart
    ON crart.code = crar.type
    JOIN clients c
    ON c.objectId = cr.clientObjectId
    LEFT JOIN expertise_profiles ep
    ON crar.educationObjectId = ep.objectId
    LEFT JOIN expertise_groups eg
    ON eg.objectId = crar.expertiseGroupObjectId

WHERE cr.reportingDate >= '2025-01-01';
