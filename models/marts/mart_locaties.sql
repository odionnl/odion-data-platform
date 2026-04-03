with hierarchie as (

    select * from {{ ref('int_locatie_hierarchie') }}

),

basis as (

    select * from {{ ref('int_locaties_basis') }}

),

kostenplaatsen as (

    -- Meest recente kostenplaats per locatie (voorkeur voor actieve koppeling)
    select
        locatie_id,
        kostenplaats_id,
        row_number() over (
            partition by locatie_id
            order by
                case
                    when einddatum_koppeling is null
                      or einddatum_koppeling > cast(getdate() as date)
                    then 0 else 1
                end,
                startdatum_koppeling desc
        ) as rn

    from {{ ref('int_locaties_met_kostenplaatsen') }}

),

locatiekoppelingen as (

    select * from {{ ref('stg_onsdb__location_assignments') }}

),

actieve_clienten as (

    select
        locatie_id,
        count(distinct client_id) as aantal_actieve_clienten
    from locatiekoppelingen
    where (einddatum is null or einddatum >= cast(getdate() as date))
    group by locatie_id

),

definitief as (

    select
        -- Sleutels & basis
        hierarchie.locatie_id,
        hierarchie.locatienaam,
        hierarchie.startdatum,
        hierarchie.einddatum,

        -- Hiërarchiepositie
        hierarchie.locatie_pad,
        hierarchie.locatie_niveau,
        hierarchie.aantal_kindlocaties,
        hierarchie.is_leaf_locatie,
        hierarchie.is_actief_vandaag                        as is_actief,
        basis.is_toplocatie,

        -- Niveaunamen
        hierarchie.niveau1,
        hierarchie.niveau2,
        hierarchie.niveau3,
        hierarchie.niveau4,
        hierarchie.niveau5,
        hierarchie.niveau6,

        -- Cluster (CC-specifiek)
        hierarchie.cluster,

        -- Locatie-eigenschappen
        basis.is_intramuraal,
        basis.capaciteit,

        -- Adres
        basis.straatnaam,
        basis.huisnummer,
        basis.postcode,
        basis.plaatsnaam,
        basis.latitude,
        basis.longitude,

        -- Kostenplaats
        kostenplaatsen.kostenplaats_id,

        -- Bezetting
        coalesce(actieve_clienten.aantal_actieve_clienten, 0) as aantal_actieve_clienten

    from hierarchie
    left join basis
        on basis.locatie_id = hierarchie.locatie_id
    left join kostenplaatsen
        on kostenplaatsen.locatie_id = hierarchie.locatie_id
        and kostenplaatsen.rn = 1
    left join actieve_clienten
        on actieve_clienten.locatie_id = hierarchie.locatie_id

)

select * from definitief
