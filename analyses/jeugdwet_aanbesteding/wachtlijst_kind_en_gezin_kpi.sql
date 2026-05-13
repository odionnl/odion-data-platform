-- KPI's voor wachtlijst Kind en Gezin sinds 2023:
-- 1. gem_unieke_clienten_per_maand
--    Gemiddeld aantal unieke cliënten op de wachtlijst per maand.
--    Snapshot op de eerste dag van de maand: leeftijd <= 8 bij start wachtlijst,
--    op die dag op de wachtlijst en nog niet in zorg gestart.
-- 2. gem_wachttijd_weken_in_zorg
--    Gemiddelde wachttijd in weken voor cliënten die in zorg zijn gekomen.
-- 3. gem_wachttijd_weken_alle
--    Gemiddelde wachttijd in weken voor alle cliënten (incl. nog niet in zorg).
--    Wachttijd niet-in-zorg = coalesce(einddatum_wachtlijst, vandaag) - startdatum_wachtlijst.
--
-- Cohort voor KPI 2 en 3: eerste wachtlijst-start >= 2023-01-01,
-- niet in zorg op dat moment, leeftijd <= 8 bij start wachtlijst.

with maanden as (

    select cast('2023-01-01' as date) as maand
    union all
    select dateadd(month, 1, maand)
    from maanden
    where maand < datefromparts(year(getdate()), month(getdate()), 1)

),

wachtlijst as (

    select
        wl.client_id,
        wl.startdatum_wachtlijst,
        wl.einddatum_wachtlijst,
        c.geboortedatum,
        datediff(year, c.geboortedatum, wl.startdatum_wachtlijst)
          - case
                when (month(wl.startdatum_wachtlijst) * 100 + day(wl.startdatum_wachtlijst))
                   < (month(c.geboortedatum) * 100 + day(c.geboortedatum))
                then 1 else 0
            end as leeftijd_bij_start_wachtlijst
    from {{ ref('mart_wachtlijsten') }} wl
    inner join {{ ref('mart_clienten') }} c
        on c.client_id = wl.client_id
    where wl.wachtlijst_niveau3 = N'Kind en Gezin (Wachtlijst)'
      and c.geboortedatum is not null

),

aantal_per_maand as (

    select
        m.maand,
        count(distinct w.client_id) as aantal
    from maanden m
    left join wachtlijst w
        on w.startdatum_wachtlijst <= m.maand
       and (w.einddatum_wachtlijst is null or w.einddatum_wachtlijst >= m.maand)
       and w.leeftijd_bij_start_wachtlijst <= 8
       and not exists (
                select 1
                from {{ ref('stg_onsdb__care_allocations') }} z
                where z.client_id = w.client_id
                  and z.startdatum <= m.maand
            )
    group by m.maand

),

eerste_wachtlijst as (

    -- Per cliënt: vroegste wachtlijst-start vanaf 2023
    select
        client_id,
        startdatum_wachtlijst,
        einddatum_wachtlijst,
        leeftijd_bij_start_wachtlijst
    from (
        select
            *,
            row_number() over (partition by client_id order by startdatum_wachtlijst) as rn
        from wachtlijst
        where startdatum_wachtlijst >= '2023-01-01'
    ) x
    where rn = 1

),

cohort as (

    -- Niet in zorg op moment van eerste wachtlijst-start, leeftijd <= 8 op dat moment.
    -- Voeg eerste zorg-start na wachtlijst toe (NULL = nooit in zorg gekomen).
    select
        w.client_id,
        w.startdatum_wachtlijst,
        w.einddatum_wachtlijst,
        w.leeftijd_bij_start_wachtlijst,
        (
            select min(z.startdatum)
            from {{ ref('stg_onsdb__care_allocations') }} z
            where z.client_id = w.client_id
              and z.startdatum >= w.startdatum_wachtlijst
        ) as eerste_zorg_start
    from eerste_wachtlijst w
    where w.leeftijd_bij_start_wachtlijst <= 8
      and not exists (
            select 1
            from {{ ref('stg_onsdb__care_allocations') }} z
            where z.client_id = w.client_id
              and z.startdatum <= w.startdatum_wachtlijst
              and (z.einddatum is null or z.einddatum > w.startdatum_wachtlijst)
        )

)

select 'gem_unieke_clienten_per_maand' as metric, avg(cast(aantal as float)) as waarde
from aantal_per_maand
union all
select 'gem_wachttijd_weken_in_zorg' as metric,
    avg(cast(datediff(week, startdatum_wachtlijst, eerste_zorg_start) as float)) as waarde
from cohort
where eerste_zorg_start is not null
union all
select 'gem_wachttijd_weken_alle' as metric,
    avg(cast(datediff(
        week,
        startdatum_wachtlijst,
        coalesce(eerste_zorg_start, einddatum_wachtlijst, cast(getdate() as date))
    ) as float)) as waarde
from cohort
