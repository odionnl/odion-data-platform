with locaties as (

    select * from {{ ref('stg_onsdb__locations') }}

),

adressen as (

    select * from {{ ref('stg_onsdb__addresses') }}

),

-- Aantal directe kindlocaties per locatie (historisch)
kindlocatie_aantallen as (

    select
        ouder_locatie_id    as locatie_id,
        count(*)            as aantal_kindlocaties
    from locaties
    where ouder_locatie_id is not null
    group by ouder_locatie_id

),

verrijkt as (

    select
        l.locatie_id,
        l.locatienaam,
        l.ouder_locatie_id,
        l.locatie_hierarchie_pad,
        l.startdatum_locatie,
        l.einddatum_locatie,
        l.is_intramuraal,
        l.capaciteit,
        l.adres_id,

        -- adresvelden
        a.straatnaam,
        a.huisnummer,
        a.postcode,
        a.plaatsnaam,
        a.latitude,
        a.longitude,

        -- hiërarchische kenmerken
        coalesce(k.aantal_kindlocaties, 0) as aantal_kindlocaties,
        case
            when coalesce(k.aantal_kindlocaties, 0) = 0 then 1
            else 0
        end as is_leaf_locatie,
        case
            when l.ouder_locatie_id is null then 1
            else 0
        end as is_toplocatie,

        -- actief vandaag
        case
            when (l.startdatum_locatie is null or l.startdatum_locatie <= cast(getdate() as date))
             and (l.einddatum_locatie is null or l.einddatum_locatie >= cast(getdate() as date))
            then 1 else 0
        end as is_actief_vandaag

    from locaties l
    left join adressen a
        on a.adres_id = l.adres_id
    left join kindlocatie_aantallen k
        on k.locatie_id = l.locatie_id

)

select * from verrijkt
