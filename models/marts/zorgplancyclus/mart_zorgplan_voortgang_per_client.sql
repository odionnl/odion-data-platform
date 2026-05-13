-- Zorgplan-voortgang per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- Per cliënt:
--   - zorgplan_versie (Nieuw/Oud/Mix/Leeg/Geen) op basis van de domeinen van
--     het Actuele zorgplan, met fallback naar Concept als er geen Actueel is
--   - zorgplan_status (Actueel/Concept/Verlopen/Geen) op basis van
--     status_omschrijving + geldigheid van het Actuele en Concept zorgplan
--   - Twee 'gearchiveerd'-checks voor oude vragenlijsten:
--     ondersteuningsvragen_gearchiveerd en persoonsbeeld_gearchiveerd.
--     Waarde 'Ja'/'Nee' wanneer zorgplan_versie='Nieuw' en
--     zorgplan_status='Actueel'; 'Niet van toepassing' anders.
--     'Ja' = geen Actuele of Concept versie van de vragenlijst meer aanwezig.
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

ondersteuningsvragen_per_client as (

    -- Aantal niet-gearchiveerde versies (status in Actueel of Concept)
    select
        client_id,
        sum(case when status in ('Actueel', 'Concept') then 1 else 0 end) as aantal_niet_gearchiveerd
    from {{ ref('mart_vragenlijst_resultaten_ondersteuningsvragen_volwassenen') }}
    group by client_id

),

persoonsbeeld_per_client as (

    -- Aantal niet-gearchiveerde versies (status in Actueel of Concept)
    select
        client_id,
        sum(case when status in ('Actueel', 'Concept') then 1 else 0 end) as aantal_niet_gearchiveerd
    from {{ ref('mart_vragenlijst_resultaten_persoonsbeeld_bejegening_signaleringsplan') }}
    group by client_id

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
        end as zorgplan_status,

        -- Tellingen voor de check (intern, niet in output)
        coalesce(ov.aantal_niet_gearchiveerd, 0) as ov_niet_gearchiveerd,
        coalesce(pb.aantal_niet_gearchiveerd, 0) as pb_niet_gearchiveerd

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join actief_zorgplan az
        on az.client_id = c.client_id
    left join concept_zorgplan cz
        on cz.client_id = c.client_id
    left join ondersteuningsvragen_per_client ov
        on ov.client_id = c.client_id
    left join persoonsbeeld_per_client pb
        on pb.client_id = c.client_id

)

select
    client_id,
    clientnummer,
    clientnaam,
    hoofdlocatie_id,
    hoofdlocatienaam,
    hoofdlocatienaam_niveau4,
    zorgplan_versie,
    zorgplan_status,

    -- Check alleen relevant bij Nieuw + Actueel zorgplan
    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when ov_niet_gearchiveerd = 0 then 'Ja' else 'Nee' end
        else 'Niet van toepassing'
    end as ondersteuningsvragen_gearchiveerd,

    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when pb_niet_gearchiveerd = 0 then 'Ja' else 'Nee' end
        else 'Niet van toepassing'
    end as persoonsbeeld_gearchiveerd

from definitief
