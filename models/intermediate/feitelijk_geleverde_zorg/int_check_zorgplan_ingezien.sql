with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

audits as (

    select * from {{ ref('int_zorgplan_inzage') }}

),

deskundigheidsgroepen as (

    select * from {{ ref('int_medewerkers_met_deskundigheidsgroepen') }}

),

dienst_locaties as (

    select * from {{ ref('int_medewerkers_met_dienst_locaties') }}

),

-- Cliëntnummer opzoeken zodat we kunnen joinen met audits
clienten_met_nummer as (

    select
        c.client_id,
        c.clientnummer

    from {{ ref('stg_onsdb__clients') }} as c
    inner join clienten
        on clienten.client_id = c.client_id

),

client_locaties as (

    select
        la.client_id,
        l.locatienaam

    from {{ ref('stg_onsdb__location_assignments') }} as la
    inner join {{ ref('stg_onsdb__locations') }} as l
        on l.locatie_id = la.locatie_id

),

-- Audits in evaluatieperiode
audits_in_periode as (

    select *
    from audits
    where tijdstip >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date))
      and tijdstip <= cast(getdate() as date)

),

-- Koppel audits aan client_id via clientnummer
audits_met_client as (

    select
        a.tijdstip,
        a.medewerker_id,
        c.client_id

    from audits_in_periode as a
    inner join clienten_met_nummer as c
        on cast(c.clientnummer as varchar(50)) collate database_default
         = cast(a.clientnummer as varchar(50)) collate database_default

),

-- Filter: medewerker moet zorgpersoneel zijn (expertise-groep actief in periode)
audits_zorgpersoneel as (

    select
        a.tijdstip,
        a.medewerker_id,
        a.client_id

    from audits_met_client as a
    inner join deskundigheidsgroepen as eg
        on eg.medewerker_id collate database_default
         = a.medewerker_id collate database_default
        and eg.deskundigheidsgroep = 'Zorgpersoneel (tbv planning & control)'
        and eg.startdatum <= cast(getdate() as date)
        and (eg.einddatum is null or eg.einddatum >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date)))

),

-- Filter: medewerker-locatie moet overlappen met client-locatie
audits_met_locatie_overlap as (

    select distinct
        az.client_id

    from audits_zorgpersoneel as az
    inner join dienst_locaties as dl
        on dl.medewerker_id collate database_default
         = az.medewerker_id collate database_default
    inner join client_locaties as cl
        on cl.client_id = az.client_id
        and cl.locatienaam collate database_default
          = dl.locatienaam collate database_default

),

definitief as (

    select
        clienten.client_id,
        case
            when audits_met_locatie_overlap.client_id is not null then 1
            else 0
        end as zorgplan_ingezien

    from clienten
    left join audits_met_locatie_overlap
        on audits_met_locatie_overlap.client_id = clienten.client_id

)

select * from definitief
