-- IGB-status per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- Per cliënt: aantallen IGB-vragenlijsten per status (Actueel / Concept /
-- Gearchiveerd), de laatste actuele IGB en een samenvattende categorie.

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

igb as (

    select * from {{ ref('mart_vragenlijst_resultaten_integratief_persoonsbeeld') }}

),

locatie_hierarchie as (

    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

igb_aggregaten as (

    select
        client_id,
        sum(case when status = 'Actueel'      then 1 else 0 end) as aantal_igb_actueel,
        sum(case when status = 'Concept'      then 1 else 0 end) as aantal_igb_concept,
        sum(case when status = 'Gearchiveerd' then 1 else 0 end) as aantal_igb_gearchiveerd
    from igb
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

        -- Aantallen per status
        coalesce(a.aantal_igb_actueel, 0)      as aantal_igb_actueel,
        coalesce(a.aantal_igb_concept, 0)      as aantal_igb_concept,
        coalesce(a.aantal_igb_gearchiveerd, 0) as aantal_igb_gearchiveerd,

        -- Samenvattende categorie
        case
            when coalesce(a.aantal_igb_actueel, 0)      > 0 then 'Actueel'
            when coalesce(a.aantal_igb_concept, 0)      > 0 then 'Concept'
            when coalesce(a.aantal_igb_gearchiveerd, 0) > 0 then 'Gearchiveerd'
            else 'Geen'
        end as igb_status

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join igb_aggregaten a
        on a.client_id = c.client_id

)

select * from definitief
