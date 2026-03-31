with clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

locaties as (

    select * from {{ ref('int_clienten_met_locaties') }}

),

huidige_locaties as (

    select
        client_id,
        locatie_id,
        locatienaam,
        is_intramuraal,
        type_toekenning,
        is_verblijfslocatie,
        row_number() over (
            partition by client_id
            order by locatie_startdatum desc
        ) as rn

    from locaties
    where locatie_einddatum is null
       or locatie_einddatum >= getdate()

),

zorglegitimaties as (

    select
        client_id,
        count(distinct zorglegitimatie_id)  as aantal_zorglegitimaties,
        min(legitimatie_startdatum)          as eerste_legitimatie_startdatum,
        max(legitimatie_einddatum)           as laatste_legitimatie_einddatum

    from {{ ref('int_zorglegitimaties_met_producten') }}
    group by client_id

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
        c.burgerlijke_staat,
        c.emailadres,
        c.mobiel_telefoonnummer,

        -- Leeftijd en leeftijdsgroep
        c.leeftijd,
        {{ get_leeftijdsgroep('c.leeftijd') }}                  as leeftijdsgroep,

        -- In zorg vlag (1 = actieve zorgtoewijzing vandaag)
        case when in_zorg.client_id is not null then 1 else 0 end as in_zorg,

        -- Huidige locatie
        huidige_locaties.locatienaam                            as huidige_locatienaam,
        huidige_locaties.is_intramuraal                         as is_intramuraal,

        -- Financiering (primaire financieringsvorm op basis van actieve zorglegitimaties)
        financiering.financiering,

        -- Zorglegitimaties
        coalesce(zorglegitimaties.aantal_zorglegitimaties, 0)   as aantal_zorglegitimaties,
        zorglegitimaties.eerste_legitimatie_startdatum,
        zorglegitimaties.laatste_legitimatie_einddatum,

        -- Deep-links naar ONS (klikbaar in Power BI)
        {{ ons_administratie_url('c.client_id') }}              as url_ons_administratie,
        {{ ons_dossier_url('care_plan', 'c.client_id') }}       as url_ons_zorgplan,
        {{ ons_dossier_url('reports', 'c.client_id') }}         as url_ons_rapportages,
        {{ ons_dossier_url('medical/overview', 'c.client_id') }} as url_ons_medicatie,
        {{ ons_dossier_url('calendar', 'c.client_id') }}        as url_ons_agenda,

        c.aangemaakt_op,
        c.gewijzigd_op

    from clienten_met_leeftijd c
    left join huidige_locaties
        on huidige_locaties.client_id = c.client_id
        and huidige_locaties.rn = 1
    left join zorglegitimaties
        on zorglegitimaties.client_id = c.client_id
    left join in_zorg
        on in_zorg.client_id = c.client_id
    left join financiering
        on financiering.client_id = c.client_id

)

select * from definitief
