-- Documenten verrijkt met leesbare statusomschrijving (lookup op lst_document_statuses).
-- Grain: één rij per document.

with documenten as (

    select document_id, status_code
    from {{ ref('stg_onsdb__documents') }}

),

document_statussen as (

    select * from {{ ref('stg_onsdb__lst_document_statuses') }}

),

definitief as (

    select
        d.document_id,
        d.status_code,
        ds.status_omschrijving

    from documenten d
    left join document_statussen ds
        on ds.status_code = d.status_code

)

select * from definitief
