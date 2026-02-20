/*==============================================================
Aantallen cliënten en gezinnen in zorg binnen cluster Kind en gezin,
per jaar (2023 t/m 2025).

Definities:
- In zorg: de zorgopname overlapte minimaal één dag met het betreffende jaar.
- Kind en gezin: de locatietoewijzing valt in cluster 'Kind en gezin'
  en overlapte eveneens minimaal één dag met het jaar.
- Gezin: uniek adres (postcode + huisnummer + toevoeging) zoals geregistreerd
  in het betreffende jaar. Cliënten zonder adres tellen elk als één afzonderlijk
  gezin (op basis van client_id).

Let op:
- Een cliënt die in hetzelfde jaar verhuisd is telt als twee afzonderlijke
  adressen in de gezinnentelling.
- Alleen woonadressen (GBA adres, adrestype_code = 1) worden meegenomen
  in de gezinnentelling.
==============================================================*/

WITH
    jaren
    AS
    (

                            SELECT 2023 AS jaar, CAST('2023-01-01' AS DATE) AS jaar_start, CAST('2023-12-31' AS DATE) AS jaar_eind
        UNION ALL
            SELECT 2024, CAST('2024-01-01' AS DATE), CAST('2024-12-31' AS DATE)
        UNION ALL
            SELECT 2025, CAST('2025-01-01' AS DATE), CAST('2025-12-31' AS DATE)

    ),

    -- Stap 1: cliënten met een zorgopname die overlapte met het jaar
    clienten_in_zorg
    AS
    (

        SELECT
            j.jaar,
            ca.client_id
        FROM [OdionDataPlatform].[odp_intermediate].[int_care_allocations]  ca
    CROSS JOIN jaren  j
        WHERE ca.startdatum_zorg <= j.jaar_eind
            AND (ca.einddatum_zorg IS NULL OR ca.einddatum_zorg >= j.jaar_start)

    ),

    -- Stap 2: cliënten met een Kind-en-gezin MAIN-locatietoewijzing die overlapte met het jaar
    locaties_kind_gezin
    AS
    (

        SELECT DISTINCT
            j.jaar,
            la.client_id
        FROM [OdionDataPlatform].[odp_intermediate].[int_location_assignments]  la
    CROSS JOIN jaren  j
            INNER JOIN [OdionDataPlatform].[odp_intermediate].[int_location_hierarchy]  lh
            ON lh.locatie_id = la.locatie_id
        WHERE la.startdatum_locatie <= j.jaar_eind
            AND la.einddatum_locatie  >= j.jaar_start
            AND lh.cluster             = N'Kind en gezin'
        --AND la.locatie_type        = 'MAIN'

    ),

    -- Stap 3: combinatie — in zorg én Kind en gezin in hetzelfde jaar
    clienten_kind_gezin
    AS
    (

        SELECT DISTINCT
            ciz.jaar,
            ciz.client_id
        FROM clienten_in_zorg  ciz
            INNER JOIN locaties_kind_gezin  lkg
            ON  lkg.client_id = ciz.client_id
                AND lkg.jaar      = ciz.jaar

    ),

    -- Stap 4: woonadres per cliënt, geldig in het betreffende jaar
    client_adressen
    AS
    (

        SELECT DISTINCT
            j.jaar,
            ca_addr.client_id,
            a.postcode,
            a.huisnummer,
            ISNULL(a.huisnummer_toevoeging, '')  AS huisnummer_toevoeging
        FROM [OdionDataPlatform].[odp_staging].[stg_ons__clients_addresses]  ca_addr
            INNER JOIN [OdionDataPlatform].[odp_staging].[stg_ons__addresses]  a
            ON a.adres_id = ca_addr.adres_id
        CROSS JOIN jaren  j
        WHERE a.adrestype_code   = 1 -- GBA adres (woonadres)
            AND a.startdatum_adres <= j.jaar_eind
            AND (a.einddatum_adres IS NULL OR a.einddatum_adres >= j.jaar_start)

    )

-- Eindresultaat: aantallen per jaar
SELECT
    ckg.jaar,
    COUNT(DISTINCT ckg.client_id)  AS aantal_clienten,
    COUNT(DISTINCT
        CASE
            WHEN adr.postcode   IS NOT NULL
        AND adr.huisnummer IS NOT NULL
            THEN adr.postcode
                 + '|' + CAST(adr.huisnummer AS VARCHAR(10))
                 + '|' + adr.huisnummer_toevoeging
            ELSE 'GEEN_ADRES|' + CAST(ckg.client_id AS VARCHAR(20))
        END
    )                              AS aantal_gezinnen

FROM clienten_kind_gezin  ckg
    LEFT JOIN client_adressen  adr
    ON  adr.client_id = ckg.client_id
        AND adr.jaar      = ckg.jaar

GROUP BY ckg.jaar
ORDER BY ckg.jaar;
