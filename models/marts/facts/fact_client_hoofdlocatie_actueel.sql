{{ config(materialized='view') }}

with client_locaties as (

    select * from {{ ref('fact_client_locatie_actueel') }}

),

final as (

    select
        client_id,
        clientnummer,
        in_zorg,
        locatie_id,
        locatienaam,
        locatie_type,
        startdatum_locatie,
        einddatum_locatie,
        locatie_pad,
        niveau1,
        niveau2,
        niveau3,
        niveau4,
        niveau5,
        niveau6
    from client_locaties
    where locatie_type = 'MAIN'

)

select * from final;
