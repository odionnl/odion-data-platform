with registraties as (

    select * from {{ ref('stg_onsdb__groupcare_registrations') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

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

definitief as (

    select

        -- Registratie
        r.groepszorg_registratie_id,
        r.registratie_datum,
        r.starttijd,
        r.eindtijd,
        CAST(ROUND(DATEDIFF(minute, r.starttijd, r.eindtijd) / 60.0, 0) AS int) as aantal_uren,
        CAST(ROUND(DATEDIFF(minute, r.starttijd, r.eindtijd) / 240.0, 0) AS int) as aantal_dagdelen,
        r.status_omschrijving as status,
        r.is_gefiatteerd,

        -- Cliënt
        c.client_id,
        c.clientnummer,
        c.clientnaam,

        -- Groep
        g.groepszorg_groep_id,
        g.groepsnaam,
        g.is_gearchiveerd                   as is_groep_gearchiveerd,
        g.min_capaciteit,
        g.max_capaciteit,

        -- Locatie van de groep
        l.locatienaam                        as groep_locatienaam,

        -- Tijdstempel
        r.aangemaakt_op,
        r.gewijzigd_op

    from registraties r
    left join clienten c
        on c.client_id = r.client_id
    left join groepen g
        on g.groepszorg_groep_id = r.groepszorg_groep_id
    left join gc_locaties gcl
        on gcl.groepszorg_locatie_id = g.groepszorg_locatie_id
    left join locaties l
        on l.locatie_id = gcl.locatie_id

)

select * from definitief
