{{ config(materialized='view') }}

with locations as (
    select
        *
    from {{ ref('int_locations_base') }}
),

hierarchy as (
    select
        *
    from {{ ref('int_location_hierarchy') }}
)

select
    -- Sleutels & basis
    locations.locatie_id,
    locations.locatienaam,
    locations.startdatum_locatie,
    locations.einddatum_locatie,

    -- Hierarchiegegevens
    hierarchy.locatie_pad,
    hierarchy.locatie_niveau,
    hierarchy.is_actief_vandaag,
    hierarchy.aantal_kindlocaties,

    -- Generieke niveaus
    hierarchy.niveau1,
    hierarchy.niveau2,
    hierarchy.niveau3,
    hierarchy.niveau4,
    hierarchy.niveau5,
    hierarchy.niveau6,

    -- Adres
    locations.straatnaam,
    locations.huisnummer,
    locations.postcode,
    locations.plaats,

    -- Clusterbepaling
    {{ get_locatiecluster('locations.locatienaam', 'hierarchy.niveau2', 'hierarchy.niveau3') }} as cluster

from locations
left join hierarchy
  on hierarchy.locatie_id = locations.locatie_id;
