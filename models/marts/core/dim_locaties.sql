-- models/marts/core/dim_locaties.sql
{{ config(materialized='view') }}

with src as (
    select
        -- Sleutels & basis
        locatie_id,
        locatienaam,
        ouder_locatie_id,
        startdatum,
        einddatum,

        -- Hierarchiegegevens
        locatie_pad,
        locatie_niveau,
        aantal_kindlocaties,
        is_leaf_locatie,
        is_actief,

        -- Generieke niveaus
        niveau1,
        niveau2,
        niveau3,
        niveau4,
        niveau5,
        niveau6
    from {{ ref('int_location_hierarchy') }}
)

select
    -- Sleutels & basis
    locatie_id,
    locatienaam,
    ouder_locatie_id,
    startdatum        as startdatum_locatie,
    einddatum         as einddatum_locatie,

    -- Hierarchiegegevens
    locatie_pad,
    locatie_niveau,
    is_actief,
    is_leaf_locatie,

    -- Generieke niveaus
    niveau1,
    niveau2,
    niveau3,
    niveau4,
    niveau5,
    niveau6,

    -- Clusterbepaling op basis van locatienaam, niveau2 en niveau3 (macro)
    {{ get_location_cluster('locatienaam', 'niveau2', 'niveau3') }} as cluster

from src;
