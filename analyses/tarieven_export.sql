USE Ons_Plan_2;

SELECT
    -- product & legitimatiecodes
    ft.id AS [Importcode legitimatie],
    p.code AS [productCode of importcode],
    p.vektiscode,

    -- debiteur info
    lst_d.description AS debtorType,
    u.code AS debtorUzoviCode,
    d.debtorNumber AS debiteurnummer,

    --agb
    dt.careProviderCode AS agbCode,

    -- tarief (value, unit, datum)
    dt.tariffValue AS priceInCents,
    lst_eu.description as priceUnit,
    dt.beginDate AS begindatum,
    dt.endDate AS eindDatum


FROM declaration_tariffs dt

    -- product & legitimatie informatie
    LEFT JOIN products p
    ON p.objectId=dt.productObjectId
    LEFT JOIN finance_types ft
    ON ft.objectId=p.financeTypeObjectId

    -- tarief-eenheid
    LEFT JOIN lst_export_units lst_eu
    ON lst_eu.code=dt.tariffUnit

    -- debiteuren
    LEFT JOIN debtors d
    ON d.objectId=dt.debtorObjectId
    LEFT JOIN lst_debtor_types lst_d
    ON lst_d.code=d.type
    LEFT JOIN uzovis u
    ON u.objectId=d.uzoviObjectId;
