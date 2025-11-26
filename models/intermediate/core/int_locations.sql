-- models/intermediate/int_locations.sql
{{ config(materialized='view') }}

with locations as (

    select
        locatie_id,
        cast(startdatum_locatie as date)  as startdatum_locatie,
        cast(einddatum_locatie  as date)  as einddatum_locatie,
        locatienaam,
        ouder_locatie_id,
        locatie_hierarchie_pad
    from {{ ref('stg_ons__locations') }}

),

-- tel hoeveel kinderen elke locatie heeft
child_counts as (

    select
        ouder_locatie_id as parent_locatie_id,
        count(*)         as aantal_kindlocaties
    from locations
    where ouder_locatie_id is not null
    group by ouder_locatie_id

),

enriched as (

    select
        l.locatie_id,
        l.startdatum_locatie,
        l.einddatum_locatie,
        l.locatienaam,
        l.ouder_locatie_id,
        l.locatie_hierarchie_pad,

        -- niveau op basis van aantal "." in het pad
        -- (bijv. "1.412.580.3")
        len(l.locatie_hierarchie_pad) 
          - len(replace(l.locatie_hierarchie_pad, '.', '')) 
          as locatie_niveau,

        -- is dit een top-level locatie (geen ouder)?
        case 
            when l.ouder_locatie_id is null then 1 
            else 0 
        end as is_toplocatie,


        -- aantal kinderen
        coalesce(c.aantal_kindlocaties, 0) as aantal_kindlocaties,

        -- is dit een leaf? (geen kinderen)
        case 
            when coalesce(c.aantal_kindlocaties, 0) = 0 then 1 
            else 0 
        end as is_leaf_locatie,

        -- is de locatie vandaag actief?
        case 
            when l.startdatum_locatie <= cast(getdate() as date)
             and (l.einddatum_locatie is null or l.einddatum_locatie >= cast(getdate() as date))
            then 1 else 0
        end as is_actief_vandaag

    from locations l
    left join child_counts c
        on c.parent_locatie_id = l.locatie_id
)

select *
from enriched;
