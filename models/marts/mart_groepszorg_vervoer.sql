-- Groepszorg-registraties gefilterd op vervoer-activiteit 'Vervoer ghz cat 0'.
-- Eén rij per registratie. Geen datum- of statusfilter in de mart zelf — Power BI
-- filtert desgewenst verder op registratie_datum / status. Testcliënten
-- (int_testclienten) zijn uitgesloten.

with registraties as (

    select * from {{ ref('stg_onsdb__groupcare_registrations') }}

),

timelines as (

    select * from {{ ref('stg_onsdb__groupcare_activity_timelines') }}

),

gc_activiteiten as (

    select * from {{ ref('stg_onsdb__groupcare_activities') }}

),

activiteiten as (

    select * from {{ ref('stg_onsdb__activities') }}

),

vervoer_registraties as (

    -- Alleen registraties waarvan de afspraak gekoppeld is aan 'Vervoer ghz cat 0'.
    -- INNER JOIN: registraties zonder afspraak-koppeling vallen weg.
    -- DISTINCT: groupcare_activity_timelines is een M:N koppeltabel; in theorie
    -- kan een afspraak meerdere keren aan dezelfde activity gekoppeld zijn.

    select distinct
        r.groepszorg_registratie_id,
        r.client_id,
        r.groepszorg_groep_id,
        r.registratie_datum,
        r.starttijd,
        r.eindtijd,
        r.status,
        r.status_omschrijving,
        r.is_gefiatteerd,
        r.aangemaakt_op,
        r.gewijzigd_op,
        a.activiteit_id,
        a.identificatienummer                   as activiteit_nummer,
        a.beschrijving                          as activiteit_beschrijving

    from registraties r
    inner join timelines t
        on t.groepszorg_afspraak_id = r.groepszorg_afspraak_id
    inner join gc_activiteiten ga
        on ga.groepszorg_activiteit_id = t.groepszorg_activiteit_id
    inner join activiteiten a
        on a.activiteit_id = ga.activiteit_id

    where a.identificatienummer = 'Vervoer ghz cat 0'

),

testclienten as (

    select client_id from {{ ref('int_testclienten') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

woonadressen as (

    -- Actief GBA-adres per cliënt. Bij meerdere actieve GBA-adressen (zou niet
    -- mogen voorkomen) verschijnt de cliënt meerdere keren in de mart.

    select *
    from {{ ref('int_clienten_met_adressen') }}
    where einddatum is null
      and adrestype like 'GBA%'

),

groepen as (

    select * from {{ ref('stg_onsdb__groupcare_groups') }}

),

gc_locaties as (

    select * from {{ ref('stg_onsdb__groupcare_locations') }}

),

locaties as (

    select * from {{ ref('stg_onsdb__locations') }}

),

adressen as (

    select * from {{ ref('stg_onsdb__addresses') }}

),

definitief as (

    select

        -- Registratie
        v.groepszorg_registratie_id,
        v.registratie_datum,
        v.starttijd,
        v.eindtijd,
        v.status,
        v.status_omschrijving,
        v.is_gefiatteerd,

        -- Activiteit
        v.activiteit_id,
        v.activiteit_nummer,
        v.activiteit_beschrijving,

        -- Cliënt
        c.client_id,
        c.clientnummer,
        c.clientnaam,
        w.adres_volledig                        as woonadres,
        w.postcode                              as woonadres_postcode,
        w.plaatsnaam                            as woonadres_plaatsnaam,

        -- Groep
        g.groepszorg_groep_id,
        g.groepsnaam,
        g.is_gearchiveerd                       as is_groep_gearchiveerd,

        -- Locatie van de groep
        loc.locatie_id                          as groep_locatie_id,
        loc.locatienaam                         as groep_locatienaam,
        adr.postcode                            as groep_postcode,
        adr.plaatsnaam                          as groep_plaatsnaam,

        -- Tijdstempel
        v.aangemaakt_op,
        v.gewijzigd_op

    from vervoer_registraties v
    left join clienten c
        on c.client_id = v.client_id
    left join woonadressen w
        on w.client_id = v.client_id
    left join groepen g
        on g.groepszorg_groep_id = v.groepszorg_groep_id
    left join gc_locaties gcl
        on gcl.groepszorg_locatie_id = g.groepszorg_locatie_id
    left join locaties loc
        on loc.locatie_id = gcl.locatie_id
    left join adressen adr
        on adr.adres_id = loc.adres_id

    where v.client_id not in (select client_id from testclienten)

)

select * from definitief
