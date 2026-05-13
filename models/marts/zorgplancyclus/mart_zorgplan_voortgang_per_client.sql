-- Zorgplan-voortgang per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- Per cliënt:
--   - zorgplan_versie (Nieuw/Oud/Mix/Leeg/Geen) op basis van de domeinen van
--     het Actuele zorgplan, met fallback naar Concept als er geen Actueel is
--   - zorgplan_status (Actueel/Concept/Verlopen/Geen) op basis van
--     status_omschrijving + geldigheid van het Actuele en Concept zorgplan
-- Invariant: een cliënt heeft maximaal 1 Actief en maximaal 1 Concept zorgplan.

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

zorgplannen as (

    select * from {{ ref('int_zorgplannen_met_versie') }}

),

locatie_hierarchie as (

    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

actief_zorgplan as (

    select
        client_id,
        geldigheid,
        zorgplan_versie
    from zorgplannen
    where status_omschrijving = 'Actief'

),

concept_zorgplan as (

    select
        client_id,
        zorgplan_versie
    from zorgplannen
    where status_omschrijving = 'Concept'

),

definitief as (

    select
        c.client_id,
        c.clientnummer,
        c.clientnaam,

        -- Locatie
        c.hoofdlocatie_id,
        c.hoofdlocatienaam,
        lh.niveau4 as hoofdlocatienaam_niveau4,

        -- Versie van het Actuele zorgplan, met Concept als fallback, anders 'Geen'
        coalesce(az.zorgplan_versie, cz.zorgplan_versie, 'Geen') as zorgplan_versie,

        -- Samenvattende status op basis van status_omschrijving + geldigheid.
        -- 'Nog niet gestart' (Actief plan met toekomstige startdatum) telt als Actueel.
        case
            when az.geldigheid in ('Geldig', 'Nog niet gestart') then 'Actueel'
            when az.geldigheid = 'Verlopen'                       then 'Verlopen'
            when cz.client_id is not null                         then 'Concept'
            else 'Geen'
        end as zorgplan_status

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join actief_zorgplan az
        on az.client_id = c.client_id
    left join concept_zorgplan cz
        on cz.client_id = c.client_id

)

select * from definitief
