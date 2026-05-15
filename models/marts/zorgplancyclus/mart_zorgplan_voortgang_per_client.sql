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
--     zorgplan_status='Actueel'; 'N.v.t.' anders.
--     'Ja' = geen Actuele of Concept versie van de vragenlijst meer aanwezig.
-- Invariant: een cliënt heeft maximaal 1 Actief en maximaal 1 Concept zorgplan.


-- TODO: Staan er maximaal twee doelen in het nieuwe zorgplan (op thuis en daginvulling, niet op verhaal)?
-- Thuis / Daginvulling = doel (tellen, norm = minimaal 1 en maximaal 2 voor het hele zorgplan, niet per domein)
-- Mijn verhaal = verhaal
-- wordt er gerapporteerd op doelen?
-- Hoe vaak wordt er op 'overige rapportage' gerapporteerd

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
    case zorgplan_versie
        when 'Nieuw' then 1
        when 'Oud'   then 2
        when 'Mix'   then 3
        when 'Leeg'  then 4
        when 'Geen'  then 4
    end as zorgplan_versie_volgorde,
    zorgplan_status,
    case zorgplan_status
        when 'Actueel'  then 1
        when 'Concept'  then 2
        when 'Verlopen' then 3
        when 'Geen'     then 4
    end as zorgplan_status_volgorde,

    -- Samenvattende voortgang-categorie op basis van versie + status.
    -- De label- en volgorde-CASE delen exact dezelfde WHEN-volgorde zodat
    -- elke voortgang-waarde 1-op-1 mapt op één volgorde-waarde (Power BI eist
    -- dit voor 'Sort by column').
    case
        when zorgplan_status = 'Geen' or zorgplan_versie = 'Leeg'         then 'Geen'
        when zorgplan_status = 'Verlopen'                                  then 'Verlopen'
        when zorgplan_versie = 'Oud'                                       then 'Oud'
        when zorgplan_status = 'Actueel' and zorgplan_versie = 'Nieuw'     then 'Nieuw & actueel'
        when zorgplan_status = 'Actueel' and zorgplan_versie = 'Mix'       then 'In ontwikkeling'
        when zorgplan_status = 'Concept' and zorgplan_versie in ('Nieuw', 'Mix') then 'In ontwikkeling'
    end as zorgplan_voortgang,
    case
        when zorgplan_status = 'Geen' or zorgplan_versie = 'Leeg'         then 5
        when zorgplan_status = 'Verlopen'                                  then 4
        when zorgplan_versie = 'Oud'                                       then 3
        when zorgplan_status = 'Actueel' and zorgplan_versie = 'Nieuw'     then 1
        when zorgplan_status = 'Actueel' and zorgplan_versie = 'Mix'       then 2
        when zorgplan_status = 'Concept' and zorgplan_versie in ('Nieuw', 'Mix') then 2
    end as zorgplan_voortgang_volgorde,

    -- Check alleen relevant bij Nieuw + Actueel zorgplan
    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when ov_niet_gearchiveerd = 0 then 'Ja' else 'Nee' end
        else 'N.v.t.'
    end as ondersteuningsvragen_gearchiveerd,
    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when ov_niet_gearchiveerd = 0 then 1 else 2 end
        else 3
    end as ondersteuningsvragen_gearchiveerd_volgorde,

    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when pb_niet_gearchiveerd = 0 then 'Ja' else 'Nee' end
        else 'N.v.t.'
    end as persoonsbeeld_gearchiveerd,
    case
        when zorgplan_versie = 'Nieuw' and zorgplan_status = 'Actueel'
            then case when pb_niet_gearchiveerd = 0 then 1 else 2 end
        else 3
    end as persoonsbeeld_gearchiveerd_volgorde

from definitief
