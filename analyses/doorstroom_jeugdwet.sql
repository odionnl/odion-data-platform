WITH
    base
    AS
    (
        SELECT
            client_id,
            legitimatie_nummer,
            financiering_code,
            financiering,
            startdatum_legitimatie,
            einddatum_legitimatie
        FROM [OdionDataPlatform].[odp_intermediate].[int_care_orders]
        WHERE financiering_code IN ('JW', 'Indicatiebesluit', 'WMO 2015', 'WMO handm')
    ),
    ordered_legitimaties
    AS
    (
        SELECT
            client_id,
            legitimatie_nummer,
            financiering_code,
            financiering,
            startdatum_legitimatie,
            einddatum_legitimatie,

            LAG(financiering) OVER (
            PARTITION BY client_id
            ORDER BY startdatum_legitimatie, einddatum_legitimatie
        ) AS vorige_legitimatie_financiering,

            LAG(financiering_code) OVER (
            PARTITION BY client_id
            ORDER BY startdatum_legitimatie, einddatum_legitimatie
        ) AS vorige_legitimatie_financiering_code,

            LAG(startdatum_legitimatie) OVER (
            PARTITION BY client_id
            ORDER BY startdatum_legitimatie, einddatum_legitimatie
        ) AS vorige_legitimatie_startdatum,

            LAG(einddatum_legitimatie) OVER (
            PARTITION BY client_id
            ORDER BY startdatum_legitimatie, einddatum_legitimatie
        ) AS vorige_legitimatie_einddatum,

            LAG(legitimatie_nummer) OVER (
            PARTITION BY client_id
            ORDER BY startdatum_legitimatie, einddatum_legitimatie
        ) AS vorige_legitimatie_nummer
        FROM base
    )
SELECT
    ol.client_id,
    c.clientnummer,

    -- 1e legitimatie (Jeugdwet)
    ol.vorige_legitimatie_nummer               AS eerste_legitimatie_nummer,
    ol.vorige_legitimatie_financiering_code    AS eerste_legitimatie_financiering,
    --ol.vorige_legitimatie_financiering         AS eerste_legitimatie_financiering,
    ol.vorige_legitimatie_startdatum           AS eerste_legitimatie_startdatum,
    ol.vorige_legitimatie_einddatum            AS eerste_legitimatie_einddatum,
    DATEDIFF(
        DAY,
        ol.vorige_legitimatie_startdatum,
        CASE
            WHEN ol.vorige_legitimatie_einddatum IS NULL THEN GETDATE()
            ELSE ol.vorige_legitimatie_einddatum
        END
    ) AS eerste_legitimatie_duur_in_dagen,

    -- 2e legitimatie (Zorgzwaartepakket)
    ol.legitimatie_nummer                      AS tweede_legitimatie_nummer,
    ol.financiering_code                       AS tweede_legitimatie_financiering,
    --ol.financiering                            AS tweede_legitimatie_financiering,
    ol.startdatum_legitimatie                  AS tweede_legitimatie_startdatum,
    ol.einddatum_legitimatie                   AS tweede_legitimatie_einddatum,
    DATEDIFF(
        DAY,
        ol.startdatum_legitimatie,
        CASE
            WHEN ol.einddatum_legitimatie IS NULL THEN GETDATE()
            ELSE ol.einddatum_legitimatie
        END
    ) AS tweede_legitimatie_duur_in_dagen,

    -- geboortedatum & leeftijd op startdatum 2e legitimatie (jaren)
    c.geboortedatum,
    DATEDIFF(YEAR, c.geboortedatum, ol.startdatum_legitimatie)
    - CASE
        WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.geboortedatum, ol.startdatum_legitimatie), c.geboortedatum)
             > ol.startdatum_legitimatie
        THEN 1 ELSE 0
      END AS leeftijd_in_jaren_bij_start_tweede_legitimatie

FROM ordered_legitimaties ol
    LEFT JOIN [OdionDataPlatform].[odp_intermediate].[int_clients] c
    ON c.client_id = ol.client_id
WHERE ol.vorige_legitimatie_financiering_code = 'JW'
    AND ol.financiering_code IN ('Indicatiebesluit', 'WMO 2015', 'WMO handm');
	--AND ol.startdatum_legitimatie >= '2020';
