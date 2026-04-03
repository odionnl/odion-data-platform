with clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

locaties as (

    select * from {{ ref('int_clienten_met_locaties') }}

),

huidige_hoofdlocatie as (

    select
        client_id,
        locatie_id,
        locatienaam,
        is_intramuraal,
        type_toekenning,
        is_verblijfslocatie

    from locaties
    where type_toekenning = 'MAIN'
      and locatie_startdatum <= cast(getdate() as date)
      and (locatie_einddatum is null
       or locatie_einddatum >= cast(getdate() as date))

),

in_zorg as (

    -- Client is "in zorg" als er een actieve zorgtoewijzing is op vandaag
    select distinct client_id
    from {{ ref('stg_onsdb__care_allocations') }}
    where startdatum <= cast(getdate() as date)
      and (einddatum is null or einddatum > cast(getdate() as date))

),

financiering as (

    select * from {{ ref('int_clienten_financiering_actueel') }}

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

        -- Leeftijd en leeftijdsgroep
        c.leeftijd,
        {{ get_leeftijdsgroep1('c.leeftijd') }}                 as leeftijdsgroep1,
        {{ get_leeftijdsgroep2('c.leeftijd') }}                 as leeftijdsgroep2,

        -- In zorg vlag (1 = actieve zorgtoewijzing vandaag)
        case when in_zorg.client_id is not null then 1 else 0 end as in_zorg,

        -- Huidige hoofdlocatie
        huidige_hoofdlocatie.locatie_id                             as huidige_hoofdlocatie_id,
        huidige_hoofdlocatie.locatienaam                            as huidige_hoofdlocatienaam,

        -- Financiering (primaire financieringsvorm op basis van actieve zorglegitimaties)
        financiering.financiering,

        c.aangemaakt_op,
        c.gewijzigd_op

    from clienten_met_leeftijd c
    left join huidige_hoofdlocatie
        on huidige_hoofdlocatie.client_id = c.client_id
    left join in_zorg
        on in_zorg.client_id = c.client_id
    left join financiering
        on financiering.client_id = c.client_id

)

select * from definitief
