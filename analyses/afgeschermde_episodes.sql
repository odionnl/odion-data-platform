USE Ons_Plan_2;

SELECT
    de.clientObjectId AS client_id,
    c.identificationNo AS clientnummer,
    de.objectId AS episode_id,
    de.title as episode_titel,
    de.startDate as startdatum,
    de.endDate as einddatum,
    de.evaluationDate as evaluatiedatum,
    de.goal as doel,
    de.important as belangrijk,
    ep.description AS deskundigheid,
    eg.name AS deskundigheidsgroep,
    CASE
        WHEN eg.name IS NULL and ep.description IS NULL THEN 0
        ELSE 1
    END AS afgeschermd,
    CASE
        WHEN eg.name IS NOT NULL AND ep.description IS NOT NULL THEN 'beiden'
        WHEN eg.name IS NULL AND ep.description IS NOT NULL THEN 'deskundigheid'
        WHEN eg.name IS NOT NULL AND ep.description IS NULL THEN 'deskundigheidsgroep'
        ELSE 'geen'
    END AS 'afgeschermd_voor'
-- TODO; rename
FROM dossier_episodes de
    LEFT JOIN dossier_expertise_authorizations dea
    ON dea.expertiseAuthorizableId = de.objectId
    LEFT JOIN expertise_profiles ep
    ON ep.objectId = dea.expertiseProfileId
    LEFT JOIN dossier_expertise_group_authorizations dega
    ON dega.expertiseGroupAuthorizableId = de.objectId
    LEFT JOIN expertise_groups eg
    ON eg.objectId = dega.expertiseGroupId
    LEFT JOIN clients c
    ON c.objectId=de.clientObjectId
WHERE de.createdAt >= '2025-01-01';
