-- =============================================================================
-- gold.fact_clienten_locaties_per_maand
-- =============================================================================

{{ config(materialized='view') }}

SELECT
    *
FROM {{ ref('fact_clienten_locaties_per_dag')}}
WHERE DATEPART(DAY,   peildatum) = 1;
