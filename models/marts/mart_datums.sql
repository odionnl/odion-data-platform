-- Datumtabel (kalender) voor Power BI: één rij per dag van 2015-01-01 t/m 31 december volgend jaar.
-- Namen in het Nederlands via CASE, zodat de uitkomst niet afhangt van de taalinstelling van SQL Server.

with cijfers as (

    select n from (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) as v(n)

),

-- 0 t/m 9999 (ruim 27 jaar)
getallen as (

    select d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000 as n
    from cijfers d1
    cross join cijfers d2
    cross join cijfers d3
    cross join cijfers d4

),

datums as (

    select dateadd(day, n, cast('2015-01-01' as date)) as datum
    from getallen
    where dateadd(day, n, cast('2015-01-01' as date)) <= datefromparts(year(getdate()) + 1, 12, 31)

),

basis as (

    select
        datum,
        year(datum)                                         as jaar,
        datepart(quarter, datum)                            as kwartaal,
        month(datum)                                        as maandnummer,
        day(datum)                                          as dag_van_maand,
        datepart(iso_week, datum)                           as iso_week,
        -- 1900-01-01 was een maandag → maandag = 1, zondag = 7
        datediff(day, cast('1900-01-01' as date), datum) % 7 + 1 as weekdagnummer

    from datums

),

definitief as (

    select
        datum,
        jaar,
        kwartaal,
        concat('Q', kwartaal)                               as kwartaal_label,
        maandnummer,
        case maandnummer
            when 1 then 'januari'   when 2 then 'februari' when 3 then 'maart'
            when 4 then 'april'     when 5 then 'mei'      when 6 then 'juni'
            when 7 then 'juli'      when 8 then 'augustus' when 9 then 'september'
            when 10 then 'oktober'  when 11 then 'november' when 12 then 'december'
        end                                                 as maandnaam,
        case maandnummer
            when 1 then 'jan' when 2 then 'feb' when 3 then 'mrt' when 4 then 'apr'
            when 5 then 'mei' when 6 then 'jun' when 7 then 'jul' when 8 then 'aug'
            when 9 then 'sep' when 10 then 'okt' when 11 then 'nov' when 12 then 'dec'
        end                                                 as maandnaam_kort,
        jaar * 100 + maandnummer                            as jaar_maand_sortering,
        concat(jaar, '-', right(concat('0', maandnummer), 2)) as jaar_maand,
        datefromparts(jaar, maandnummer, 1)                 as maand_start,
        dag_van_maand,
        iso_week,
        weekdagnummer,
        case weekdagnummer
            when 1 then 'maandag'  when 2 then 'dinsdag' when 3 then 'woensdag'
            when 4 then 'donderdag' when 5 then 'vrijdag' when 6 then 'zaterdag'
            when 7 then 'zondag'
        end                                                 as weekdagnaam,
        case when weekdagnummer in (6, 7) then 1 else 0 end as is_weekend,
        case when datum = cast(getdate() as date) then 1 else 0 end as is_vandaag,
        case when datum <= cast(getdate() as date) then 1 else 0 end as is_tot_en_met_vandaag

    from basis

)

select * from definitief
