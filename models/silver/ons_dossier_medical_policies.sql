-- =============================================================================
-- silver.ons_dossier_medical_policies
-- =============================================================================

{{ config(materialized='table', as_columnstore=false) }}

select
    clientObjectId           as client_id,
    employeeObjectId         as employee_id,
    snomedExpressionValue    as snomed_expression,
    documentName             as document_name,
    narrative,
    cast(createdAt as datetime) as created_at,
    cast(updatedAt as datetime) as updated_at,
    cast([date] as date)     as document_date
from {{ source('ons_plan_2', 'dossier_medical_policies') }}
where createdAt is not null
