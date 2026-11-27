-- Ons Agenda-afspraakmomenten: één rij per afspraak per datum (herhalende reeksen uitgevouwen).
-- Venster: 2 jaar terug t/m 3 maanden vooruit (zie int_agenda_afspraak_momenten).
-- Kenmerken van de afspraak (titel, uursoort, labels, herhaling) staan in mart_agenda_afspraken (join op afspraak_id).
-- Testcliënt-uitsluiting volgt uit de inner join op mart_agenda_afspraken.
-- Grain: één rij per afspraakmoment (afspraak_id + datum).

with momenten as (

    select * from {{ ref('int_agenda_afspraak_momenten') }}

),

afspraken as (

    select afspraak_id from {{ ref('mart_agenda_afspraken') }}

),

definitief as (

    select
        m.afspraak_moment_id,
        m.afspraak_id,
        m.reeks_id,
        m.datum,
        m.starttijd,
        m.eindtijd,
        m.duur_minuten,
        cast(m.duur_minuten / 60.0 as decimal(10, 2))   as duur_uren,
        m.is_herhalend

    from momenten m
    inner join afspraken a
        on a.afspraak_id = m.afspraak_id

)

select * from definitief
