-- =============================================================================
-- gold.fact_leeftijd_clienten_per_maand
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    *
FROM {{ ref('fact_leeftijd_clienten_per_dag')}}
WHERE DATEPART(DAY,   peildatum) = 1;
