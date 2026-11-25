-- Ons Agenda-afspraken uitgevouwen tot losse afspraakmomenten (één rij per afspraak per datum).
--
-- Bewuste uitzondering op "intermediate = all-time": herhalende reeksen hebben geen natuurlijk einde,
-- dus uitvouwen gebeurt binnen een venster van 2 jaar terug t/m 3 maanden vooruit t.o.v. GETDATE().
--
-- Herhalingsregels (herhalingstype, hoofdletterongevoelig; basisdatum = cycleBase, anders geldig_vanaf):
--   single          → eenmalig op geldig_vanaf
--   daily           → elke <interval> dagen vanaf de basisdatum
--   weekly          → op de aangevinkte weekdagen, elke <interval> weken vanaf de week van de basisdatum
--   monthly_on_day  → elke <interval> maanden op dezelfde dag van de maand als de basisdatum
--                     (maanden zonder die dag, bv. de 31e, worden overgeslagen)
--   monthly_on_week → elke <interval> maanden op dezelfde weekdag in dezelfde week van de maand
--                     (bv. 2e dinsdag); basisdatum in week 5 = laatste die weekdag van de maand
--   yearly          → elke <interval> jaar op dezelfde dag en maand (29 februari alleen in schrikkeljaren)
--   overig          → eenmalig op geldig_vanaf (onbekende types; de staging-test waarschuwt hiervoor)
-- Uitzonderingsrijen (is_uitzondering = 1) leveren geen moment op en schrappen het moment van
-- herhalende afspraken in dezelfde reeks op die datum. Losse (aangepaste) afspraken in dezelfde reeks blijven staan.
--
-- Grain: één rij per afspraak_id + datum.

with afspraken as (

    select * from {{ ref('stg_onsdb__agenda_events') }}

),

cijfers as (

    select n from (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) as v(n)

),

-- 0 t/m 9999 dagen; ruim voldoende voor het venster
getallen as (

    select d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000 as n
    from cijfers d1
    cross join cijfers d2
    cross join cijfers d3
    cross join cijfers d4

),

datums as (

    select dateadd(day, n, dateadd(year, -2, cast(getdate() as date))) as datum
    from getallen
    where dateadd(day, n, dateadd(year, -2, cast(getdate() as date)))
          <= dateadd(month, 3, cast(getdate() as date))

),

datums_verrijkt as (

    select
        datum,
        -- 1900-01-01 was een maandag → maandag = 1, zondag = 7
        datediff(day, cast('1900-01-01' as date), datum) % 7 + 1                        as weekdagnummer,
        dateadd(day, -(datediff(day, cast('1900-01-01' as date), datum) % 7), datum)   as maandag_van_week,
        (day(datum) - 1) / 7 + 1                                                        as week_van_maand,
        case when day(datum) + 7 > day(eomonth(datum)) then 1 else 0 end                as is_laatste_weekdag_van_maand

    from datums

),

reeksen as (

    select
        a.afspraak_id,
        a.reeks_id,
        a.starttijd_seconden,
        a.duur_seconden,
        cast(a.geldig_vanaf as date)                                    as geldig_vanaf,
        cast(a.geldig_tot as date)                                      as geldig_tot,
        lower(ltrim(rtrim(coalesce(a.herhalingstype, 'single'))))       as herhaling,
        case when coalesce(a.herhaling_interval, 0) < 1 then 1
             else a.herhaling_interval end                              as herhaling_interval,
        b.basisdatum,
        dateadd(day, -(datediff(day, cast('1900-01-01' as date), b.basisdatum) % 7), b.basisdatum)
                                                                        as basis_maandag,
        datediff(day, cast('1900-01-01' as date), b.basisdatum) % 7 + 1 as basis_weekdagnummer,
        (day(b.basisdatum) - 1) / 7 + 1                                 as basis_week_van_maand,
        datediff(day, cast('1900-01-01' as date), cast(a.geldig_vanaf as date)) % 7 + 1
                                                                        as weekdag_geldig_vanaf,
        a.is_maandag,
        a.is_dinsdag,
        a.is_woensdag,
        a.is_donderdag,
        a.is_vrijdag,
        a.is_zaterdag,
        a.is_zondag

    from afspraken a
    cross apply (
        select cast(coalesce(a.herhaling_basisdatum, a.geldig_vanaf) as date) as basisdatum
    ) b

    where a.is_uitzondering = 0
      and a.geldig_vanaf is not null

),

uitzonderingen as (

    select distinct
        reeks_id,
        cast(geldig_vanaf as date) as datum

    from afspraken
    where is_uitzondering = 1

),

eenmalig as (

    select
        r.afspraak_id,
        r.reeks_id,
        r.starttijd_seconden,
        r.duur_seconden,
        r.herhaling,
        d.datum

    from reeksen r
    inner join datums_verrijkt d
        on d.datum = r.geldig_vanaf

    where r.herhaling not in ('daily', 'weekly', 'monthly_on_day', 'monthly_on_week', 'yearly')

),

herhalend as (

    select
        r.afspraak_id,
        r.reeks_id,
        r.starttijd_seconden,
        r.duur_seconden,
        r.herhaling,
        d.datum

    from reeksen r
    inner join datums_verrijkt d
        on d.datum >= r.geldig_vanaf
        and (r.geldig_tot is null or d.datum <= r.geldig_tot)

    where
        (
            r.herhaling = 'daily'
            and abs(datediff(day, r.basisdatum, d.datum)) % r.herhaling_interval = 0
        )
        or
        (
            r.herhaling = 'weekly'
            and abs(datediff(day, r.basis_maandag, d.maandag_van_week) / 7) % r.herhaling_interval = 0
            and (
                case d.weekdagnummer
                    when 1 then r.is_maandag
                    when 2 then r.is_dinsdag
                    when 3 then r.is_woensdag
                    when 4 then r.is_donderdag
                    when 5 then r.is_vrijdag
                    when 6 then r.is_zaterdag
                    when 7 then r.is_zondag
                end = 1
                -- geen weekdag aangevinkt: val terug op de weekdag van de eerste afspraak
                or (
                    coalesce(r.is_maandag, 0) + coalesce(r.is_dinsdag, 0) + coalesce(r.is_woensdag, 0)
                    + coalesce(r.is_donderdag, 0) + coalesce(r.is_vrijdag, 0)
                    + coalesce(r.is_zaterdag, 0) + coalesce(r.is_zondag, 0) = 0
                    and d.weekdagnummer = r.weekdag_geldig_vanaf
                )
            )
        )
        or
        (
            r.herhaling = 'monthly_on_day'
            and abs(datediff(month, r.basisdatum, d.datum)) % r.herhaling_interval = 0
            and day(d.datum) = day(r.basisdatum)
        )
        or
        (
            r.herhaling = 'monthly_on_week'
            and abs(datediff(month, r.basisdatum, d.datum)) % r.herhaling_interval = 0
            and d.weekdagnummer = r.basis_weekdagnummer
            and (
                (r.basis_week_van_maand < 5 and d.week_van_maand = r.basis_week_van_maand)
                or (r.basis_week_van_maand = 5 and d.is_laatste_weekdag_van_maand = 1)
            )
        )
        or
        (
            r.herhaling = 'yearly'
            and abs(datediff(year, r.basisdatum, d.datum)) % r.herhaling_interval = 0
            and month(d.datum) = month(r.basisdatum)
            and day(d.datum) = day(r.basisdatum)
        )

),

herhalend_zonder_uitzonderingen as (

    select h.*
    from herhalend h
    where not exists (
        select 1
        from uitzonderingen u
        where u.reeks_id = h.reeks_id
          and u.datum = h.datum
    )

),

momenten as (

    select * from eenmalig
    union all
    select * from herhalend_zonder_uitzonderingen

),

definitief as (

    select
        concat(afspraak_id, '-', convert(char(8), datum, 112))                      as afspraak_moment_id,
        afspraak_id,
        reeks_id,
        datum,
        dateadd(second, cast(coalesce(starttijd_seconden, 0) as int), cast(datum as datetime2(0)))
                                                                                    as starttijd,
        dateadd(
            second,
            cast(coalesce(starttijd_seconden, 0) + coalesce(duur_seconden, 0) as int),
            cast(datum as datetime2(0))
        )                                                                           as eindtijd,
        cast(coalesce(duur_seconden, 0) / 60.0 as decimal(10, 2))                   as duur_minuten,
        case
            when herhaling in ('daily', 'weekly', 'monthly_on_day', 'monthly_on_week', 'yearly') then 1
            else 0
        end                                                                         as is_herhalend

    from momenten

)

select * from definitief
