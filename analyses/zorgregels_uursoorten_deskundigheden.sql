USE Ons_Plan_2;

SELECT TOP (1000)
    pl.objectId,
    pl.removed              AS verwijderd,
    pl.verified             AS gefiatteerd,
    pl.clientId             AS clientObjectId,
    c.identificationNo      AS clientnummer,
    pl.startDate            AS startdatum,
    pl.endDate              AS einddatum,
    e.identificationNo      AS medewerkernummer,
    e.firstName             AS voornaam,
    e.lastName              AS achternaam,
    ep.description          AS deskundigheid,
    a.description           AS uursoort
FROM presence_logs pl
    INNER JOIN employees e
    ON e.objectId = pl.employeeId
    LEFT JOIN activities a
    ON a.objectId = pl.activityObjectId
    LEFT JOIN expertise_profile_assignments epa
    ON epa.employeeObjectId = pl.employeeId
        AND epa.startTime <= pl.startDate
        AND (epa.endTime >= pl.endDate OR epa.endTime IS NULL)
    LEFT JOIN expertise_profiles ep
    ON ep.objectId = epa.expertiseProfileObjectId
        AND ep.visible = 1
    LEFT JOIN clients c
    ON c.objectId = pl.clientId
WHERE
	pl.startDate >= '2025'
    AND pl.verified = 0
    --AND pl.clientId = 267 