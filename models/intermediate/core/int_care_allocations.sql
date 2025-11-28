{{ config(materialized='view') }}

with src as (

    select * from {{ ref('stg_ons__care_allocations') }}

),


final as (

    select
        src.zorgtoewijzing_id,
        src.client_id,
        src.startdatum_zorg,
        coalesce(src.einddatum_zorg, cast('9999-12-31' as date)) as einddatum_zorg,
        src.reden_uit_zorg,
        src.opmerkingen
    from src

)

select * from final;
