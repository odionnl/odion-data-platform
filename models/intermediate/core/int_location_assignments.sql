{{ config(materialized='view') }}

with src as (

    select * from {{ ref('stg_ons__location_assignments') }}

),


final as (

    select
        locatietoewijzing_id,
        client_id,
        locatie_id,
        startdatum_locatie,
        coalesce(src.einddatum_locatie, cast('9999-12-31' as date)) as einddatum_locatie,
        locatie_type
    from src

)

select * from final;
