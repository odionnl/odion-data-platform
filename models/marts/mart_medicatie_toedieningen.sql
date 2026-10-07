-- Alle medicatietoedieningen (t/m vandaag) met status en hoofdlocatie van de cliënt op de geplande datum.
-- Testcliënten (int_testclienten) zijn uitgesloten.
-- Grain: één rij per toediening.

with testclienten as (

    select * from {{ ref('int_testclienten') }}

),

toedieningen as (

    select * from {{ ref('int_medicatie_toedieningen') }}
    where client_id not in (select client_id from testclienten)
      and ingepland_op < dateadd(day, 1, cast(getdate() as date))

),

locatiekoppelingen as (

    select * from {{ ref('stg_onsdb__location_assignments') }}
    where type_toekenning = 'MAIN'

),

-- Toedienlijsten worden opnieuw gegenereerd: alleen het laatste overzicht per client+datum telt
huidige_overzichten as (

    select
        medication_chart_id,
        row_number() over (
            partition by client_id, overzicht_datum
            order by overzicht_gegenereerd_op desc, medication_chart_id desc
        ) as rn

    from (
        select distinct client_id, overzicht_datum, overzicht_gegenereerd_op, medication_chart_id
        from toedieningen
    ) overzichten

),

relevante_toedieningen as (

    select
        t.toediening_id,
        t.client_id,
        t.ingepland_op,
        cast(t.ingepland_op as date) as ingepland_datum,
        t.status,
        t.status_gewijzigd_op

    from toedieningen t
    inner join huidige_overzichten ho
        on ho.medication_chart_id = t.medication_chart_id
        and ho.rn = 1

),

-- Hoofdlocatie geldig op de geplande datum; bij overlap de meest recent gestarte koppeling
toediening_locatie as (

    select
        rt.toediening_id,
        lk.locatie_id,
        row_number() over (
            partition by rt.toediening_id
            order by lk.startdatum desc, lk.locatiekoppeling_id desc
        ) as rn

    from relevante_toedieningen rt
    inner join locatiekoppelingen lk
        on lk.client_id = rt.client_id
        and lk.startdatum <= rt.ingepland_datum
        and (lk.einddatum is null or lk.einddatum >= rt.ingepland_datum)

),

definitief as (

    select
        -- Toediening
        rt.toediening_id,
        rt.client_id,
        rt.ingepland_op,
        rt.ingepland_datum,

        -- Status
        rt.status,
        case rt.status
            when 'administered'   then 'Toegediend'
            when 'handed'         then 'Aangereikt'
            when 'prepared'       then 'Klaargezet'
            when 'self_managed'   then 'In eigen beheer'
            when 'failed'         then 'Niet toegediend'
            when 'none_scheduled' then 'Geen toediening gepland'
            when 'unknown'        then 'Onbekend'
            -- NULL = ingepland maar nooit afgetekend; onbekende nieuwe waarden vallen door (test vangt dit)
            else coalesce(rt.status, 'Niet afgetekend')
        end as status_omschrijving,
        case when rt.status is not null then 1 else 0 end as is_afgetekend,
        case
            when rt.status in ('handed', 'administered', 'prepared', 'self_managed') then 1
            else 0
        end as is_verstrekt,
        rt.status_gewijzigd_op,
        cast(rt.status_gewijzigd_op as date) as afgetekend_datum,

        -- Hoofdlocatie op geplande datum (details via mart_locaties)
        tl.locatie_id

    from relevante_toedieningen rt
    left join toediening_locatie tl
        on tl.toediening_id = rt.toediening_id
        and tl.rn = 1

)

select * from definitief
