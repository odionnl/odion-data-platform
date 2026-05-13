-- Aantal unieke cliënten op wachtlijst Kind en Gezin per maand sinds 2023.
-- Snapshot op de eerste dag van de maand: cliënt staat dan op de wachtlijst
-- en is op die dag nog niet in zorg gestart.
-- Splitsing op leeftijd bij start wachtlijst:
--   - aantal_unieke_clienten_tm8     : leeftijd t/m 8 jaar bij start wachtlijst (de doelgroep)
--   - aantal_unieke_clienten_ouder   : leeftijd > 8 jaar bij start wachtlijst (check)

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

bezetting_per_maand as (

    select
        m.maand,
        w.client_id,
        w.leeftijd_bij_start_wachtlijst
    from maanden m
    inner join wachtlijst w
        on w.startdatum_wachtlijst <= m.maand
       and (w.einddatum_wachtlijst is null or w.einddatum_wachtlijst >= m.maand)
       and not exists (
                select 1
                from {{ ref('stg_onsdb__care_allocations') }} z
                where z.client_id = w.client_id
                  and z.startdatum <= m.maand
            )

)

select
    m.maand,
    count(distinct case when b.leeftijd_bij_start_wachtlijst <= 8 then b.client_id end) as aantal_unieke_clienten_tm8,
    count(distinct case when b.leeftijd_bij_start_wachtlijst >  8 then b.client_id end) as aantal_unieke_clienten_ouder
from maanden m
left join bezetting_per_maand b
    on b.maand = m.maand
group by m.maand
order by m.maand
