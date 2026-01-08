USE Ons_Plan_2;

SELECT
    r.clientObjectId,
    c.identificationNo,
    lst.description AS relationType,
    r.firstName,
    r.birthName,
    r.initials,
    r.birthNamePrefix,
    r.name,
    r.prefix,
    r.organization,
    r.comments,
    r.createdAt,
    r.updatedAt
FROM relations r
    LEFT JOIN clients c ON c.objectId=r.clientObjectId
    LEFT JOIN lst_wlz_cod472s lst ON lst.code=r.type
WHERE r.type=14
    AND r.updatedAt >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)
-- alles vanaf begin vorige maand tot nu
ORDER BY updatedAt ASC

