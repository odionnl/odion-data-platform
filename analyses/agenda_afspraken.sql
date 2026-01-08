USE Ons_Plan_2;

WITH
    -- één rij per (event, locatie)
    location_invites
    AS
    (
        SELECT DISTINCT
            i.eventObjectId         AS eventObjectId,
            ol.externalObjectId     AS locationExternalObjectId
        FROM dbo.agenda_invitations i
            JOIN dbo.onsagenda_locations ol
            ON ol.objectId = i.inviteeObjectId
    ),
    -- aantal cliënten per afspraak (alleen afspraken met > 1 cliënt)
    client_counts
    AS
    (
        SELECT
            i.eventObjectId                 AS eventObjectId,
            COUNT(DISTINCT oc.objectId)     AS aantal_clienten
        FROM dbo.agenda_invitations i
            JOIN dbo.onsagenda_clients oc
            ON oc.objectId = i.inviteeObjectId
        GROUP BY i.eventObjectId
        HAVING COUNT(DISTINCT oc.objectId) > 1
    )
SELECT
    e.objectId                      AS eventObjectId,
    li.locationExternalObjectId     AS locationObjectId,
    l.name                          AS locatie,
    e.name                          AS afspraaktitel,
    e.comment						AS commentaar,
    e.createdAt                     AS afspraak_aangemaakt,
    e.validFrom                     AS afspraak_start,
    e.validTo                       AS afspraak_einde,
    e.clientPresent                 AS client_aanwezig,
    cc.aantal_clienten              AS aantal_clienten
FROM dbo.agenda_events e
    LEFT JOIN location_invites li
    ON li.eventObjectId = e.objectId -- alleen events met locatie
    LEFT JOIN client_counts cc
    ON cc.eventObjectId = e.objectId -- alleen events met > 1 cliënt
    LEFT JOIN dbo.locations l
    ON l.objectId = li.locationExternalObjectId
WHERE aantal_clienten > 1
    AND (e.validTo IS NULL OR e.validTo >= GETDATE())
ORDER BY
    e.validFrom,
    e.objectId;
