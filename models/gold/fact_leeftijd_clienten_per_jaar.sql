-- =============================================================================
-- gold.fact_leeftijd_clienten_per_jaar
-- =============================================================================


{{ config(materialized='view') }}

SELECT
        *
    FROM {{ ref('fact_leeftijd_clienten_per_dag') }}
    WHERE DATEPART(DAY,   peildatum) = 1
        AND DATEPART(MONTH, peildatum) = 1;
