-- Levensloop-status per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- Per cliënt: aantallen Levensloop-vragenlijsten per status
-- (Actueel / Concept / Gearchiveerd) en een samenvattende categorie.
-- Een Actuele Levensloop die meer dan 1 jaar niet is bijgewerkt
-- (gewijzigd_op > 1 jaar geleden) krijgt categorie 'Verlopen'.

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

levensloop as (

    select * from {{ ref('mart_vragenlijst_resultaten_levensloop') }}

),

locatie_hierarchie as (

    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

levensloop_aggregaten as (

    select
        client_id,
        sum(case when status = 'Actueel'      then 1 else 0 end) as aantal_levensloop_actueel,
        sum(case when status = 'Concept'      then 1 else 0 end) as aantal_levensloop_concept,
        sum(case when status = 'Gearchiveerd' then 1 else 0 end) as aantal_levensloop_gearchiveerd,
        max(case when status = 'Actueel' then gewijzigd_op end)  as laatst_bijgewerkt_actueel
    from levensloop
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
        coalesce(a.aantal_levensloop_actueel, 0)      as aantal_levensloop_actueel,
        coalesce(a.aantal_levensloop_concept, 0)      as aantal_levensloop_concept,
        coalesce(a.aantal_levensloop_gearchiveerd, 0) as aantal_levensloop_gearchiveerd,

        -- Laatste wijziging op een Actuele Levensloop
        a.laatst_bijgewerkt_actueel,

        -- Samenvattende categorie
        case
            when coalesce(a.aantal_levensloop_actueel, 0) > 0
                 and a.laatst_bijgewerkt_actueel < dateadd(year, -1, cast(getdate() as date))
                then 'Verlopen'
            when coalesce(a.aantal_levensloop_actueel, 0)      > 0 then 'Actueel'
            when coalesce(a.aantal_levensloop_concept, 0)      > 0 then 'Concept'
            when coalesce(a.aantal_levensloop_gearchiveerd, 0) > 0 then 'Gearchiveerd'
            else 'Geen'
        end as levensloop_status

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join levensloop_aggregaten a
        on a.client_id = c.client_id

)

select * from definitief
