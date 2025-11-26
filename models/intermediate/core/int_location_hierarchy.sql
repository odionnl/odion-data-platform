-- models/intermediate/core/int_location_hierarchy.sql
{{ config(materialized='view') }}

with H as (
    -- Anchor: rootlocaties (geen ouder)
    select
        l.locatie_id,                         -- Uniek ID van de locatie
        l.ouder_locatie_id,                   -- Bovenliggende locatie

        cast(l.startdatum_locatie as date) as startdatum,
        cast(l.einddatum_locatie   as date) as einddatum,

        cast(l.locatienaam as nvarchar(255)) collate database_default as locatienaam,

        -- Niveau-kolommen (allemaal nvarchar(255) + zelfde collation)
        cast(l.locatienaam as nvarchar(255)) collate database_default as niveau1,
        cast(null as nvarchar(255))          collate database_default as niveau2,
        cast(null as nvarchar(255))          collate database_default as niveau3,
        cast(null as nvarchar(255))          collate database_default as niveau4,
        cast(null as nvarchar(255))          collate database_default as niveau5,
        cast(null as nvarchar(255))          collate database_default as niveau6,

        1 as niveau,   -- huidig niveau (root = 1)

        cast('.' + cast(l.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as pad,

        -- verrijking uit int_locations direct meenemen
        l.aantal_kindlocaties,
        l.is_leaf_locatie,
        l.is_actief_vandaag
    from {{ ref('int_locations') }} l
    where l.ouder_locatie_id is null

    union all

    -- Recursief deel: children
    select
        c.locatie_id,
        c.ouder_locatie_id,

        cast(c.startdatum_locatie as date) as startdatum,
        cast(c.einddatum_locatie   as date) as einddatum,

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

        cast(h.pad + cast(c.locatie_id as nvarchar(50)) + '.' as nvarchar(1000)) as pad,

        c.aantal_kindlocaties,
        c.is_leaf_locatie,
        c.is_actief_vandaag
    from {{ ref('int_locations') }} c
    join H
      on c.ouder_locatie_id = H.locatie_id
    where h.niveau < 6
)

select
    locatie_id,
    locatienaam,
    ouder_locatie_id,
    startdatum,
    einddatum,
    pad              as locatie_pad,
    niveau           as locatie_niveau,
    aantal_kindlocaties,
    is_leaf_locatie,
    is_actief_vandaag,
    niveau1,
    niveau2,
    niveau3,
    niveau4,
    niveau5,
    niveau6
from H;
