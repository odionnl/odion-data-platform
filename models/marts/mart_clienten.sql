with testclienten as (

    select * from {{ ref('int_testclienten') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}
    where client_id not in (select client_id from testclienten)

),

in_zorg as (

    -- Client is "in zorg" als er een actieve zorgtoewijzing is op vandaag
    select distinct client_id
    from {{ ref('stg_onsdb__care_allocations') }}
    where startdatum <= cast(getdate() as date)
      and (einddatum is null or einddatum > cast(getdate() as date))

),

clienten_met_leeftijd as (

    -- Leeftijdsberekening: corrigeert voor verjaardag die dit jaar nog niet is geweest
    select
        c.*,
        datediff(year, c.geboortedatum, cast(getdate() as date))
        - case
            when (month(cast(getdate() as date)) * 100 + day(cast(getdate() as date)))
               < (month(c.geboortedatum) * 100 + day(c.geboortedatum))
            then 1 else 0
          end as leeftijd

    from clienten c

),

definitief as (

    select
        c.client_id,
        c.clientnummer,
        c.voornaam,
        c.roepnaam,
        c.initialen,
        c.voorvoegsel,
        c.achternaam,
        c.geboortenaam,
        c.partnernaam,
        c.clientnaam,
        c.geboortedatum,
        c.overlijdensdatum,
        c.geslacht,
        c.emailadres,
        c.mobiel_telefoonnummer,

        -- Leeftijd (-1 = onbekend, voor join met mart_leeftijdsgroepen)
        coalesce(c.leeftijd, -1) as leeftijd,

        -- In zorg vlag (1 = actieve zorgtoewijzing vandaag)
        case when in_zorg.client_id is not null then 1 else 0 end as is_in_zorg,

        c.aangemaakt_op,
        c.gewijzigd_op

    from clienten_met_leeftijd c
    left join in_zorg
        on in_zorg.client_id = c.client_id

)

select * from definitief
