/*==============================================================
Selecteert alle apothekers met hun meest recente adresgegevens.
==============================================================*/
USE Ons_Plan_2;

WITH
    addr
    AS
    (
        SELECT
            cpa.careProviderObjectId,
            a.*,
            ROW_NUMBER() OVER (
      PARTITION BY cpa.careProviderObjectId
      ORDER BY a.createdAt DESC
    ) AS rn
        FROM care_provider_addresses cpa
            JOIN addresses a
            ON a.objectId = cpa.addressObjectId
        WHERE NULLIF(TRIM(a.street), '') IS NOT NULL
            AND a.beginDate <= GETDATE()
            AND (a.endDate >= GETDATE() OR a.endDate IS NULL)
    )
SELECT
    cp.fullName AS naam,
    --cp.id,
    a.street AS straatnaam,
    a.homeNumber AS huisnummer,
    a.homeNumberExtension AS huisnummer_toevoeging,
    a.zipcode AS postcode,
    a.city AS gemeente,
    a.telephoneNumber AS telefoonnummer,
    a.email
FROM care_providers cp
    JOIN care_provider_categories cpc
    ON cpc.objectId = cp.organisationCategoryId
        AND cpc.name = 'Apothekers'
    LEFT JOIN addr a
    ON a.careProviderObjectId = cp.objectId
        AND a.rn = 1
