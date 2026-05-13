-- Analyse: alle zorglegitimatie-productregels voor IGB of PPG die op enig moment sinds 2023 liepen
-- (einddatum vanaf 2023-01-01, of nog open). Eén regel per zorglegitimatie-product.

select
    zlp.zorglegitimatie_product_id,
    zlp.zorglegitimatie_id,
    zlp.product_id,
    zlp.product_code,
    zlp.product_omschrijving,
    zlp.hoeveelheid_in_minuten,
    zlp.startdatum,
    zlp.einddatum,
    zlp.startdatum_origineel,
    zlp.einddatum_origineel,
    zlp.zorglegitimatie_type,
    zlp.legitimatienummer,
    zlp.startdatum_legitimatie,
    zlp.einddatum_legitimatie,
    zlp.client_id,
    zlp.clientnummer,
    zlp.client_naam,
    zlp.financieringstype_naam,
    zlp.is_actief,
    zlp.aangemaakt_op,
    zlp.gewijzigd_op

from {{ ref('mart_zorglegitimatie_producten') }} zlp

where (zlp.product_omschrijving like '%PPG%' or zlp.product_omschrijving like '%IGB%')
  and (zlp.einddatum >= '2023-01-01' or zlp.einddatum is null)

order by zlp.client_id, zlp.startdatum
