with trainingslocatie_clienten as (

    -- Cliënten met (historische) koppeling onder niveau2 '99. Trainingslocatie'
    select distinct la.client_id
    from {{ ref('stg_onsdb__location_assignments') }} la
    inner join {{ ref('int_locatie_hierarchie') }} lh
        on lh.locatie_id = la.locatie_id
    where lh.niveau2 = '99. Trainingslocatie'

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}
    -- Testcliënten uitsluiten: vaste clientnummers + trainingslocatie
    where clientnummer not in ('10510', '11428')
      and client_id not in (select client_id from trainingslocatie_clienten)

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
