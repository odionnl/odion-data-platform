-- IPB-status per cliënt in zorg (snapshot vandaag).
-- Grain: één rij per cliënt uit mart_clienten_actueel.
-- IPB is van toepassing voor cliënten die voldoen aan minstens één van:
--   - actief ZZP-product uit de IPB-doelgroep
--     (VG 5-8, LG 4-7, ZG aud 2-3, ZG vis 2-3)
--   - hoofdlocatie hangt onder niveau4 in de IPB-locatielijst
--     (Dynamica ODC, Boomgaard ODC de, Gezinsbehandeling)
-- Voor overige cliënten is ipb_status 'Niet van toepassing'.

with clienten as (

    select * from {{ ref('mart_clienten_actueel') }}

),

ipb as (

    select * from {{ ref('mart_vragenlijst_resultaten_integratief_persoonsbeeld') }}

),

producten_actueel as (

    select * from {{ ref('mart_zorglegitimatie_producten_actueel') }}

),

locatie_hierarchie as (

    select
        locatie_id,
        niveau4
    from {{ ref('int_locatie_hierarchie') }}

),

actuele_productcodes_per_client as (

    select
        client_id,
        string_agg(product_code, ' | ')
            within group (order by product_code) as actuele_productcodes
    from (
        select distinct client_id, product_code
        from producten_actueel
        where product_code is not null
    ) p
    group by client_id

),

ipb_van_toepassing as (

    -- Via ZZP-product uit de IPB-doelgroep
    select distinct client_id
    from producten_actueel
    where financieringstype_product = 'Zorgzwaartepakket'
      and product_code in (
          'VG 5', 'VG 6', 'VG 7', 'VG 8',
          'LG 4', 'LG 5', 'LG 6', 'LG 7',
          'ZG aud 2', 'ZG aud 3',
          'ZG vis 2', 'ZG vis 3'
      )

    union

    -- Via hoofdlocatie (niveau4 in de IPB-locatielijst)
    select c.client_id
    from clienten c
    inner join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    where lh.niveau4 in (
        'Dynamica ODC',
        'Boomgaard ODC, de',
        'Gezinsbehandeling'
    )

),

ipb_aggregaten as (

    select
        client_id,
        sum(case when status = 'Actueel'      then 1 else 0 end) as aantal_ipb_actueel,
        sum(case when status = 'Concept'      then 1 else 0 end) as aantal_ipb_concept,
        sum(case when status = 'Gearchiveerd' then 1 else 0 end) as aantal_ipb_gearchiveerd
    from ipb
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

        -- Actuele producten
        pc.actuele_productcodes,

        -- IPB-doelgroep
        case when vt.client_id is not null then 1 else 0 end as ipb_van_toepassing,

        -- Aantallen per status
        coalesce(a.aantal_ipb_actueel, 0)      as aantal_ipb_actueel,
        coalesce(a.aantal_ipb_concept, 0)      as aantal_ipb_concept,
        coalesce(a.aantal_ipb_gearchiveerd, 0) as aantal_ipb_gearchiveerd,

        -- Samenvattende categorie
        case
            when vt.client_id is null                       then 'Niet van toepassing'
            when coalesce(a.aantal_ipb_actueel, 0)      > 0 then 'Actueel'
            when coalesce(a.aantal_ipb_concept, 0)      > 0 then 'Concept'
            when coalesce(a.aantal_ipb_gearchiveerd, 0) > 0 then 'Gearchiveerd'
            else 'Geen'
        end as ipb_status

    from clienten c
    left join locatie_hierarchie lh
        on lh.locatie_id = c.hoofdlocatie_id
    left join actuele_productcodes_per_client pc
        on pc.client_id = c.client_id
    left join ipb_van_toepassing vt
        on vt.client_id = c.client_id
    left join ipb_aggregaten a
        on a.client_id = c.client_id

)

select * from definitief
