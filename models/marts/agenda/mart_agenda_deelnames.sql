-- Deelnames aan Ons Agenda-afspraakmomenten: wie (medewerker, cliënt, locatie, team, groep) bij welk moment.
-- Feitentabel voor uren per medewerker/cliënt/team/uursoort.
-- Koppelt via afspraak_moment_id aan mart_agenda_afspraak_momenten en via afspraak_id aan mart_agenda_afspraken.
-- Venster: 2 jaar terug t/m 3 maanden vooruit (volgt mart_agenda_afspraak_momenten).
-- AVG: geen namen, alleen ID's — join in Power BI op mart_medewerkers / mart_clienten.
-- Testcliënt-uitsluiting volgt uit mart_agenda_afspraak_momenten.
-- Grain: één rij per afspraakmoment × uitnodiging.

with momenten as (

    select * from {{ ref('mart_agenda_afspraak_momenten') }}

),

genodigden as (

    select * from {{ ref('int_agenda_genodigden') }}

),

activiteiten as (

    select * from {{ ref('stg_onsdb__activities') }}

),

definitief as (

    select
        -- Sleutels
        concat(g.uitnodiging_id, '-', convert(char(8), m.datum, 112))  as deelname_id,
        m.afspraak_moment_id,
        m.afspraak_id,
        g.uitnodiging_id,

        -- Moment
        m.datum,
        m.starttijd,
        m.eindtijd,
        m.duur_uren,

        -- Genodigde
        g.genodigde_type,
        g.medewerker_id,
        g.client_id,
        g.locatie_id,
        g.team_id,
        g.groepszorg_groep_id,
        g.is_geaccepteerd,
        g.is_vervangen,
        g.oorspronkelijke_medewerker_id,

        -- Uursoort (uitnodiging gaat voor afspraak)
        g.activiteit_id,
        act.beschrijving                                                as uursoort_beschrijving,

        -- Zorglegitimatie
        g.zorglegitimatie_id

    from momenten m
    inner join genodigden g
        on g.afspraak_id = m.afspraak_id
    left join activiteiten act
        on act.activiteit_id = g.activiteit_id

)

select * from definitief
