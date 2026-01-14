USE Ons_Plan_2;

WITH
    tags_cte
    AS
    (
        SELECT
            dt.documentObjectId,
            STUFF((
            SELECT DISTINCT ' | ' + t2.name
            FROM document_tags dt2
                JOIN tags t2
                ON t2.objectId = dt2.tagObjectId
            WHERE dt2.documentObjectId = dt.documentObjectId
            FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 3, '') AS labels
        FROM document_tags dt
        GROUP BY dt.documentObjectId
    )
SELECT
    d.clientObjectId as client_id,
    c.identificationNo as clientnummer,
    d.objectId AS document_id,
    d.filename AS documentnaam,
    d.createdAt AS aangemaakt_op,
    d.updatedAt AS bijgewerkt_op,
    ds.description AS status,
    tags_cte.labels AS labels,
    ep.description AS deskundigheid,
    eg.name AS deskundigheidsgroep,
    d.confidential AS afgeschermd,
    CASE
        WHEN eg.name IS NOT NULL AND ep.description IS NOT NULL THEN 'beiden'
        WHEN eg.name IS NULL AND ep.description IS NOT NULL THEN 'deskundigheid'
        WHEN eg.name IS NOT NULL AND ep.description IS NULL THEN 'deskundigheidsgroep'
        ELSE 'geen'
    END AS 'afgeschermd_voor'
FROM documents d
    LEFT JOIN document_rights dr
    ON dr.documentObjectId = d.objectId
    JOIN clients c
    ON c.objectId = d.clientObjectId
    LEFT JOIN expertise_profiles ep
    ON ep.objectId = dr.educationObjectId
    LEFT JOIN document_expertise_groups deg
    ON deg.documentObjectId = d.objectId
    LEFT JOIN expertise_groups eg
    ON eg.objectId = deg.expertiseGroupObjectId
    LEFT JOIN lst_document_statuses ds
    ON ds.code = d.status
    LEFT JOIN tags_cte
    ON tags_cte.documentObjectId = d.objectId
WHERE d.updatedAt >= '2025-01-01'
;
