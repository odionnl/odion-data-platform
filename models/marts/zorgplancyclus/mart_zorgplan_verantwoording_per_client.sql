-- Zorgplan-verantwoording-status per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- Per cliënt: aantallen vragenlijsten "Zorgplan, welke keuzes hebben we gemaakt?"
-- per status (Actueel / Concept / Gearchiveerd) en een samenvattende categorie.
-- Een Actuele vragenlijst die meer dan 1 jaar niet is bijgewerkt
-- (gewijzigd_op > 1 jaar geleden) krijgt categorie 'Verlopen'.

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

zorgplan_verantwoording as (

    select * from {{ ref('mart_vragenlijst_resultaten_zorgplan_verantwoording') }}

),

locatie_hierarchie as (

    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

zorgplan_verantwoording_aggregaten as (

    select
        client_id,
        sum(case when status = 'Actueel'      then 1 else 0 end) as aantal_zorgplan_verantwoording_actueel,
        sum(case when status = 'Concept'      then 1 else 0 end) as aantal_zorgplan_verantwoording_concept,
        sum(case when status = 'Gearchiveerd' then 1 else 0 end) as aantal_zorgplan_verantwoording_gearchiveerd,
        max(case when status = 'Actueel' then gewijzigd_op end)  as laatst_bijgewerkt_actueel
    from zorgplan_verantwoording
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
        coalesce(a.aantal_zorgplan_verantwoording_actueel, 0)      as aantal_zorgplan_verantwoording_actueel,
        coalesce(a.aantal_zorgplan_verantwoording_concept, 0)      as aantal_zorgplan_verantwoording_concept,
        coalesce(a.aantal_zorgplan_verantwoording_gearchiveerd, 0) as aantal_zorgplan_verantwoording_gearchiveerd,

        -- Laatste wijziging op een Actuele vragenlijst
        a.laatst_bijgewerkt_actueel,

        -- Samenvattende categorie + sorteervolgorde (zelfde WHEN-volgorde
        -- voor 1-op-1 mapping in Power BI 'Sort by column')
        case
            when coalesce(a.aantal_zorgplan_verantwoording_actueel, 0) > 0
                 and a.laatst_bijgewerkt_actueel < dateadd(year, -1, cast(getdate() as date))
                then 'Verlopen'
            when coalesce(a.aantal_zorgplan_verantwoording_actueel, 0)      > 0 then 'Actueel'
            when coalesce(a.aantal_zorgplan_verantwoording_concept, 0)      > 0 then 'Concept'
            when coalesce(a.aantal_zorgplan_verantwoording_gearchiveerd, 0) > 0 then 'Gearchiveerd'
            else 'Geen'
        end as zorgplan_verantwoording_status,
        case
            when coalesce(a.aantal_zorgplan_verantwoording_actueel, 0) > 0
                 and a.laatst_bijgewerkt_actueel < dateadd(year, -1, cast(getdate() as date))
                then 3
            when coalesce(a.aantal_zorgplan_verantwoording_actueel, 0)      > 0 then 1
            when coalesce(a.aantal_zorgplan_verantwoording_concept, 0)      > 0 then 2
            when coalesce(a.aantal_zorgplan_verantwoording_gearchiveerd, 0) > 0 then 4
            else 5
        end as zorgplan_verantwoording_status_volgorde

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join zorgplan_verantwoording_aggregaten a
        on a.client_id = c.client_id

)

select * from definitief
