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

enriched as (

    select
        l.*,

        -- adresvelden
        a.straatnaam,
        a.huisnummer,
        a.postcode,
        a.plaats,

        -- locatie-niveau (op basis van pad)
        len(l.locatie_hierarchie_pad)
          - len(replace(l.locatie_hierarchie_pad, '.', ''))
          as locatie_niveau,

        -- toplocatie of niet
        case when l.ouder_locatie_id is null then 1 else 0 end as is_toplocatie,

        -- locatie actief of niet
        case
            when l.startdatum_locatie <= cast(getdate() as date)
             and (l.einddatum_locatie is null or l.einddatum_locatie >= cast(getdate() as date))
            then 1 else 0
        end as is_actief_vandaag

    from locations l
    left join addresses a
      on a.adres_id = l.adres_id
)

select * from enriched;
