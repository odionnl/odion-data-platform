{{ config(materialized='view') }}

with locations as (

    select
        *
    from {{ ref('stg_ons__locations') }}

),

addresses as (

    select
        *
    from {{ ref('stg_ons__addresses') }}
),

lst_location_types as (

    select
        *
    from {{ ref('stg_ons__lst_location_types') }}
),


-- directe kinderen (alle kinderen, historisch)
child_counts_all as (
    select
        ouder_locatie_id as locatie_id,
        count(*) as aantal_kindlocaties
    from locations
    where ouder_locatie_id is not null
    group by ouder_locatie_id
),

enriched as (

    select
        l.*,

        -- adresvelden
        a.straatnaam,
        a.huisnummer,
        a.postcode,
        a.plaats,

        -- locatietype (kamer of locatie)
        lt.locatie_type,

        -- afgeleide kolommen over hierarchische gegevens 
        len(l.locatie_hierarchie_pad)
          - len(replace(l.locatie_hierarchie_pad, '.', ''))
          as locatie_niveau,
        case when l.ouder_locatie_id is null then 1 else 0 end as is_toplocatie,
        coalesce(c.aantal_kindlocaties, 0) as aantal_kindlocaties,
        case when coalesce(c.aantal_kindlocaties, 0) = 0 then 1 else 0 end as is_leaf_locatie,

        -- locatie actief of niet
        case
            when l.startdatum_locatie <= cast(getdate() as date)
             and (l.einddatum_locatie is null or l.einddatum_locatie >= cast(getdate() as date))
            then 1 else 0
        end as is_actief_vandaag

    from locations l
    left join addresses a
      on a.adres_id = l.adres_id
    left join lst_location_types lt
        on lt.locatie_type_code = l.locatie_type_code
    left join child_counts_all c
      on c.locatie_id = l.locatie_id
)

select * from enriched;
