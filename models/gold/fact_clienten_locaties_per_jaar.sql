-- =============================================================================
-- gold.fact_clienten_locaties_per_jaar
-- =============================================================================

{{ config(materialized='view') }}


SELECT
    *
FROM {{ ref('fact_clienten_locaties_per_dag')}}
WHERE DATEPART(DAY,   peildatum) = 1
    AND DATEPART(MONTH, peildatum) = 1;
