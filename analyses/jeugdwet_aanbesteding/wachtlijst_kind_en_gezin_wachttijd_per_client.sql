-- Wachttijd per cliënt op wachtlijst Kind en Gezin
-- Cohort: eerste wachtlijst-start vanaf 2023, leeftijd <= 8 bij start wachtlijst,
-- cliënt niet in zorg op dat moment.
-- Wachttijd = weken tussen eerste wachtlijst-start en eerste daaropvolgende start in zorg
-- of, als nog niet in zorg, tot einddatum_wachtlijst of vandaag.
-- Kolom is_in_zorg_gekomen: 1 als cliënt zorg heeft gekregen, 0 anders.

with wachtlijst as (

    select
        wl.client_id,
        wl.startdatum_wachtlijst,
        wl.einddatum_wachtlijst,
        wl.wachtlijst_locatienaam,
        c.geboortedatum
    from {{ ref('mart_wachtlijsten') }} wl
    inner join {{ ref('mart_clienten') }} c
        on c.client_id = wl.client_id
    where wl.wachtlijst_niveau3 = N'Kind en Gezin (Wachtlijst)'
      and c.geboortedatum is not null

),

eerste_wachtlijst as (

    select
        client_id,
        startdatum_wachtlijst,
        einddatum_wachtlijst,
        wachtlijst_locatienaam,
        geboortedatum,
        datediff(year, geboortedatum, startdatum_wachtlijst)
          - case
                when (month(startdatum_wachtlijst) * 100 + day(startdatum_wachtlijst))
                   < (month(geboortedatum)         * 100 + day(geboortedatum))
                then 1 else 0
            end as leeftijd_bij_start_wachtlijst
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

    select
        w.*,
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

select
    co.client_id,
    c.clientnummer,
    co.wachtlijst_locatienaam,
    co.startdatum_wachtlijst,
    co.einddatum_wachtlijst,
    co.eerste_zorg_start,
    co.leeftijd_bij_start_wachtlijst,
    case when co.eerste_zorg_start is not null then 1 else 0 end as is_in_zorg_gekomen,
    datediff(
        week,
        co.startdatum_wachtlijst,
        coalesce(co.eerste_zorg_start, co.einddatum_wachtlijst, cast(getdate() as date))
    ) as wachttijd_weken
from cohort co
inner join {{ ref('mart_clienten') }} c
    on c.client_id = co.client_id
order by c.clientnummer
