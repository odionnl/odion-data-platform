-- models/intermediate/core/int_location_hierarchy.sql
{{ config(materialized='view') }}

with H as (
    -- Anchor: rootlocaties (geen ouder)
    select
        l.locatie_id,                         -- Uniek ID van de locatie
        l.ouder_locatie_id,                   -- Bovenliggende locatie
        l.startdatum_locatie  as startdatum,  -- Startdatum van locatie
        l.einddatum_locatie   as einddatum,   -- Einddatum van locatie

        cast(l.locatienaam as nvarchar(255)) collate database_default as locatienaam,

        -- Niveau-kolommen (allemaal nvarchar(255) + zelfde collation)
        cast(l.locatienaam as nvarchar(255)) collate database_default as niveau1,
        cast(null as nvarchar(255))          collate database_default as niveau2,
        cast(null as nvarchar(255))          collate database_default as niveau3,
        cast(null as nvarchar(255))          collate database_default as niveau4,
        cast(null as nvarchar(255))          collate database_default as niveau5,
        cast(null as nvarchar(255))          collate database_default as niveau6,

        1 as niveau,   -- huidig niveau (root = 1)

        cast('.' + cast(l.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as pad
    from {{ ref('stg_ons__locations') }} l
    where l.ouder_locatie_id is null

    union all

    -- Recursief deel: children
    select
        c.locatie_id,
        c.ouder_locatie_id,
        c.startdatum_locatie  as startdatum,
        c.einddatum_locatie   as einddatum,

        cast(c.locatienaam as nvarchar(255)) collate database_default as locatienaam,

        -- niveaus doorschuiven, gebaseerd op h.niveau
        h.niveau1,

        cast(
            case when h.niveau = 1 
                 then c.locatienaam 
                 else h.niveau2 
            end 
            as nvarchar(255)
        ) collate database_default as niveau2,

        cast(
            case when h.niveau = 2 
                 then c.locatienaam 
                 else h.niveau3 
            end 
            as nvarchar(255)
        ) collate database_default as niveau3,

        cast(
            case when h.niveau = 3 
                 then c.locatienaam 
                 else h.niveau4 
            end 
            as nvarchar(255)
        ) collate database_default as niveau4,

        cast(
            case when h.niveau = 4 
                 then c.locatienaam 
                 else h.niveau5 
            end 
            as nvarchar(255)
        ) collate database_default as niveau5,

        cast(
            case when h.niveau = 5 
                 then c.locatienaam 
                 else h.niveau6 
            end 
            as nvarchar(255)
        ) collate database_default as niveau6,

        h.niveau + 1 as niveau,

        cast(h.pad + cast(c.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as pad
    from {{ ref('stg_ons__locations') }} c
    join H
      on c.ouder_locatie_id = H.locatie_id
    where h.niveau < 6
),

enriched as (
    select
        h.locatie_id,
        h.locatienaam,
        h.ouder_locatie_id,
        h.startdatum,
        h.einddatum,

        -- jouw eigen, canonieke pad
        h.pad          as locatie_pad,

        -- 1 kolom voor niveau (vanuit de CTE)
        h.niveau       as locatie_niveau,

        -- verrijking uit int_locations (zonde om weg te gooien)
        l.aantal_kindlocaties,
        l.is_leaf_locatie,
        l.is_actief,

        -- namen per niveau (handig voor marts)
        h.niveau1,
        h.niveau2,
        h.niveau3,
        h.niveau4,
        h.niveau5,
        h.niveau6
    from H
    left join {{ ref('int_locations') }} l
      on l.locatie_id = h.locatie_id
)

select
    locatie_id,
    locatienaam,
    ouder_locatie_id,
    startdatum,
    einddatum,
    locatie_pad,
    locatie_niveau,
    aantal_kindlocaties,
    is_leaf_locatie,
    is_actief,
    niveau1,
    niveau2,
    niveau3,
    niveau4,
    niveau5,
    niveau6
from enriched;
