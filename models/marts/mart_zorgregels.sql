with zorgregels as (

    select * from {{ ref('stg_onsdb__presence_logs') }}
    where starttijd >= datefromparts(year(getdate()), 1, 1)

),

activiteiten as (

    select * from {{ ref('stg_onsdb__activities') }}

),

medewerkers as (

    select
        medewerker_id,
        personeelsnummer,
        concat(voornaam, ' ', achternaam) as medewerker_naam
    from {{ ref('stg_onsdb__employees') }}

),

clienten as (

    select
        client_id,
        clientnummer
    from {{ ref('stg_onsdb__clients') }}

),

teams as (

    select
        team_id,
        teamnaam
    from {{ ref('stg_onsdb__teams') }}

),

definitief as (

    select
        z.zorgregel_id,
        z.client_id,
        c.clientnummer,
        z.medewerker_id,
        m.personeelsnummer,
        m.medewerker_naam,
        z.activiteit_id,
        a.beschrijving                              as uursoort_beschrijving,
        a.is_werktijd,
        a.is_direct,
        a.is_reistijd,
        z.team_id,
        t.teamnaam,

        z.starttijd,
        z.eindtijd,
        cast(z.starttijd as date)                   as datum,
        datediff(minute, z.starttijd, z.eindtijd)   as duur_minuten,

        z.is_gefiatteerd,
        z.gefiatteerd_op,

        z.is_urenregistratie,
        z.is_verloning,
        z.heeft_tijdsduur,
        z.is_automatisch_verdeeld,

        z.aangemaakt_op,
        z.gewijzigd_op

    from zorgregels z
    left join activiteiten a
        on a.activiteit_id = z.activiteit_id
    left join medewerkers m
        on m.medewerker_id = z.medewerker_id
    left join clienten c
        on c.client_id = z.client_id
    left join teams t
        on t.team_id = z.team_id

)

select * from definitief
