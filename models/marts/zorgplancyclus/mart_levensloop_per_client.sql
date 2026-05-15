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

nieuwe_vragen_per_client as (

    -- Heeft de cliënt op een Actuele Levensloop een gevuld tekst_antwoord voor
    -- elk van de drie 'nieuwe' vragen?
    select
        client_id,
        max(case
            when vraagtekst = 'Welke praktische informatie is belangrijk om te noteren?'
            then 1 else 0
        end) as heeft_praktische_info,
        max(case
            when vraagtekst = 'Is er sprake van een gebeurtenis die nu nog invloed heeft op het dagelijks leven van de cliënt?'
            then 1 else 0
        end) as heeft_gebeurtenis,
        max(case
            when vraagtekst = 'Heeft de cliënt wensen voor de palliatieve fase van het leven en de uitvaart?'
            then 1 else 0
        end) as heeft_palliatieve_wensen
    from {{ ref('mart_vragenlijst_antwoorden_levensloop') }}
    where status = 'Actueel'
      and tekst_antwoord is not null
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

        -- Drie 'nieuwe' vragen volledig ingevuld op Actuele Levensloop.
        -- 'N.v.t.' als de cliënt geen Actuele Levensloop heeft.
        case
            when coalesce(a.aantal_levensloop_actueel, 0) = 0 then 'N.v.t.'
            when coalesce(nv.heeft_praktische_info, 0) = 1
             and coalesce(nv.heeft_gebeurtenis, 0) = 1
             and coalesce(nv.heeft_palliatieve_wensen, 0) = 1
            then 'Ja' else 'Nee'
        end as nieuwe_vragen_gevuld,
        case
            when coalesce(a.aantal_levensloop_actueel, 0) = 0 then 3
            when coalesce(nv.heeft_praktische_info, 0) = 1
             and coalesce(nv.heeft_gebeurtenis, 0) = 1
             and coalesce(nv.heeft_palliatieve_wensen, 0) = 1
            then 1 else 2
        end as nieuwe_vragen_gevuld_volgorde,

        -- Samenvattende categorie + sorteervolgorde (zelfde WHEN-volgorde
        -- voor 1-op-1 mapping in Power BI 'Sort by column')
        case
            when coalesce(a.aantal_levensloop_actueel, 0) > 0
                 and a.laatst_bijgewerkt_actueel < dateadd(year, -1, cast(getdate() as date))
                then 'Verlopen'
            when coalesce(a.aantal_levensloop_actueel, 0)      > 0 then 'Actueel'
            when coalesce(a.aantal_levensloop_concept, 0)      > 0 then 'Concept'
            when coalesce(a.aantal_levensloop_gearchiveerd, 0) > 0 then 'Gearchiveerd'
            else 'Geen'
        end as levensloop_status,
        case
            when coalesce(a.aantal_levensloop_actueel, 0) > 0
                 and a.laatst_bijgewerkt_actueel < dateadd(year, -1, cast(getdate() as date))
                then 3
            when coalesce(a.aantal_levensloop_actueel, 0)      > 0 then 1
            when coalesce(a.aantal_levensloop_concept, 0)      > 0 then 2
            when coalesce(a.aantal_levensloop_gearchiveerd, 0) > 0 then 4
            else 5
        end as levensloop_status_volgorde

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join levensloop_aggregaten a
        on a.client_id = c.client_id
    left join nieuwe_vragen_per_client nv
        on nv.client_id = c.client_id

)

select * from definitief
