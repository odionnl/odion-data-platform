-- =============================================================================
-- gold.dim_clienten
-- =============================================================================
{{ config(
    materialized='view'
) }}

SELECT
  c.objectId         AS clientObjectId,
  c.identificationNo AS clientnummer,
  c.dateOfBirth      AS geboortedatum,
  c.deathDate        AS overlijdensdatum
FROM {{ source('silver', 'ons_clients') }} AS c;
