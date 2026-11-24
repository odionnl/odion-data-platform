with locaties as (

    select * from {{ ref('mart_locaties') }}

),

testclienten as (

    select * from {{ ref('int_testclienten') }}

),

kamers as (

    select
        locatie_id              as kamer_id,
        locatienaam             as kamernaam,
        ouder_locatie_id,
        capaciteit,
        startdatum              as startdatum_kamer,
        einddatum               as einddatum_kamer,
        is_actief               as is_actieve_kamer

    from locaties
    where locatietype = 'Kamer'
      -- Kamers op de trainingslocatie zijn geen echte woningen
      and (niveau2 is null or niveau2 <> '99. Trainingslocatie')

),

ouders as (

    select
        locatie_id              as ouder_locatie_id,
        locatienaam             as ouder_locatienaam

    from locaties

),

koppelingen as (

    -- Koppelingen van testcliënten tellen niet als bezetting; de kamer blijft wel staan
    select * from {{ ref('stg_onsdb__location_assignments') }}
    where client_id is null
       or client_id not in (select client_id from testclienten)

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

definitief as (

    select
        -- Kamer
        kamers.kamer_id,
        kamers.kamernaam,
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
