-- Analyse zorgplan-status per cliënt in zorg.
-- Grain: één rij per cliënt in zorg (vanuit mart_clienten_actueel).
-- Een cliënt heeft maximaal één zorgplan met status 'Actief' en maximaal één
-- zorgplan met status 'Concept' (systeem-invariant).

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

zorgplannen as (

    select * from {{ ref('int_zorgplannen_met_versie') }}

),

locatie_hierarchie as (

    -- Voor elke locatie de bijbehorende niveau4-naam (ancestor of zichzelf)
    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

zorgtoewijzingen_actueel as (

    -- Per cliënt max 1 actieve zorgtoewijzing (systeem-invariant)
    select
        client_id,
        startdatum as startdatum_in_zorg
    from {{ ref('stg_onsdb__care_allocations') }}
    where startdatum <= cast(getdate() as date)
      and (einddatum is null or einddatum > cast(getdate() as date))

),

actief_zorgplan as (

    select
        client_id,
        startdatum,
        einddatum,
        geldigheid
    from zorgplannen
    where status_omschrijving = 'Actief'

),

concept_zorgplan as (

    select
        client_id,
        startdatum,
        einddatum,
        gewijzigd_op
    from zorgplannen
    where status_omschrijving = 'Concept'

),

definitief as (

    select
        c.client_id,
        c.clientnummer,
        c.clientnaam,
        c.hoofdlocatie_id,
        c.hoofdlocatienaam,
        lh.niveau4 as hoofdlocatienaam_niveau4,

        -- In zorg
        za.startdatum_in_zorg,
        case
            when za.startdatum_in_zorg is not null
             and datediff(day, za.startdatum_in_zorg, cast(getdate() as date)) > 42 then 1
            else 0
        end as langer_dan_6_weken_in_zorg,

        -- Actief zorgplan
        case when az.client_id is not null then 1 else 0 end as actief_zorgplan_aanwezig,
        coalesce(az.geldigheid, 'Geen') as actief_zorgplan_geldigheid,
        az.startdatum                   as startdatum_actief_zorgplan,
        az.einddatum                    as einddatum_actief_zorgplan,
        case
            when az.einddatum is null then null
            else datediff(day, az.startdatum, az.einddatum) / 7
        end                             as actief_zorgplan_geldigheidsduur_weken,
        case
            when az.client_id is null                                            then null
            when az.geldigheid = 'Geldig'
             and az.einddatum is not null
             and az.einddatum <= dateadd(day, 56, cast(getdate() as date))       then 1
            else 0
        end as verloopt_binnen_8_weken,
        case
            when az.client_id is null                                            then null
            when az.geldigheid = 'Verlopen'
             and datediff(day, az.einddatum, cast(getdate() as date)) > 56       then 1
            else 0
        end as langer_dan_8_weken_verlopen,

        -- Concept zorgplan
        case when cz.client_id is not null then 1 else 0 end as concept_zorgplan_aanwezig,
        cz.startdatum                   as startdatum_concept_zorgplan,
        cz.einddatum                    as einddatum_concept_zorgplan,
        cz.gewijzigd_op                 as concept_zorgplan_laatst_gewijzigd,
        case
            when cz.client_id is null                                         then null
            when datediff(day, cz.gewijzigd_op, cast(getdate() as date)) > 56 then 1
            else 0
        end as langer_dan_8_weken_geleden_bewerkt,

        -- Samenvatting
        case
            when az.geldigheid = 'Geldig'                             then 'Actief & geldig zorgplan'
            when az.geldigheid = 'Verlopen' and cz.client_id is not null then 'Verlopen zorgplan met concept'
            when az.geldigheid = 'Verlopen'                           then 'Verlopen zorgplan zonder concept'
            when az.geldigheid = 'Nog niet gestart'                   then 'Nog niet gestart'
            when az.client_id is null and cz.client_id is not null    then 'Alleen concept'
            else 'Geen zorgplan'
        end as client_zorgplan_status

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join zorgtoewijzingen_actueel za
        on za.client_id = c.client_id
    left join actief_zorgplan az
        on az.client_id = c.client_id
    left join concept_zorgplan cz
        on cz.client_id = c.client_id

)

select * from definitief
