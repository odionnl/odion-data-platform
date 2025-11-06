-- =============================================================================
-- gold.fact_leeftijd_clienten_actueel
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    *
FROM {{ ref('fact_leeftijd_clienten_per_dag')}}
WHERE peildatum = CAST(GETDATE() AS date);
