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
        az.startdatum                   as actief_zorgplan_startdatum,
        az.einddatum                    as actief_zorgplan_einddatum,
        case
            when az.geldigheid = 'Verlopen'
             and datediff(day, az.einddatum, cast(getdate() as date)) > 56 then 1
            else 0
        end as langer_dan_8_weken_verlopen,

        -- Concept zorgplan
        case when cz.client_id is not null then 1 else 0 end as concept_zorgplan_aanwezig,
        cz.startdatum                   as concept_zorgplan_startdatum,
        cz.einddatum                    as concept_zorgplan_einddatum,
        cz.gewijzigd_op                 as concept_zorgplan_laatst_gewijzigd,

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
    left join zorgtoewijzingen_actueel za
        on za.client_id = c.client_id
    left join actief_zorgplan az
        on az.client_id = c.client_id
    left join concept_zorgplan cz
        on cz.client_id = c.client_id

)

select * from definitief
