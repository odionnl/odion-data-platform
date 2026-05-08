with locaties as (

    select * from {{ ref('mart_locaties') }}

),

kamers as (

    select
        locatie_id              as kamer_id,
        locatienaam             as kamernaam,
        case
            when lower(locatienaam) like '%(logeer)%' then 'Logeren'
            else 'Wonen'
        end                     as kamertype,
        ouder_locatie_id,
        capaciteit,
        startdatum              as startdatum_kamer,
        einddatum               as einddatum_kamer,
        is_actief               as is_actieve_kamer

    from locaties
    where locatietype = 'Kamer'

),

ouders as (

    select
        locatie_id              as ouder_locatie_id,
        locatienaam             as ouder_locatienaam

    from locaties

),

koppelingen as (

    select * from {{ ref('stg_onsdb__location_assignments') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

definitief as (

    select
        -- Kamer
        kamers.kamer_id,
        kamers.kamernaam,
        kamers.kamertype,
        kamers.capaciteit,
        kamers.startdatum_kamer,
        kamers.einddatum_kamer,
        kamers.is_actieve_kamer,

        -- Ouderlocatie
        kamers.ouder_locatie_id,
        ouders.ouder_locatienaam,

        -- Koppeling
        koppelingen.locatiekoppeling_id,
        koppelingen.startdatum,
        koppelingen.einddatum,
        case
            when koppelingen.locatiekoppeling_id is null then 0
            when (koppelingen.startdatum is null or koppelingen.startdatum <= cast(getdate() as date))
             and (koppelingen.einddatum  is null or koppelingen.einddatum  >= cast(getdate() as date))
            then 1 else 0
        end as is_actieve_koppeling,

        -- Client
        koppelingen.client_id,
        clienten.clientnummer,
        clienten.clientnaam

    from kamers
    left join ouders
        on ouders.ouder_locatie_id = kamers.ouder_locatie_id
    left join koppelingen
        on koppelingen.locatie_id = kamers.kamer_id
    left join clienten
        on clienten.client_id = koppelingen.client_id

)

select * from definitief
