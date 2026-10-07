with toedienafspraken as (

    select * from {{ ref('stg_onsdb__administration_agreements') }}

),

toedieningen as (

    select * from {{ ref('stg_onsdb__medication_administrations') }}

),

statusupdates as (

    select * from {{ ref('stg_onsdb__status_updates') }}

),

overzichten as (

    select * from {{ ref('stg_onsdb__medication_charts') }}

),

-- status_updates is een historie van overgangen: alleen de laatste telt
laatste_status as (

    select
        toediening_id,
        status,
        aangemaakt_op,
        row_number() over (
            partition by toediening_id
            order by aangemaakt_op desc, statusupdate_id desc
        ) as rn

    from statusupdates

),

definitief as (

    select
        toedieningen.toediening_id,
        overzichten.client_id,
        toedienafspraken.medication_chart_id,
        overzichten.gegenereerd_op      as overzicht_gegenereerd_op,
        overzichten.datum               as overzicht_datum,
        toedieningen.ingepland_op,
        laatste_status.status,
        laatste_status.aangemaakt_op    as status_gewijzigd_op

    from toedienafspraken
    inner join toedieningen
        on toedieningen.toedienafspraak_id = toedienafspraken.toedienafspraak_id
    inner join overzichten
        on overzichten.medication_chart_id = toedienafspraken.medication_chart_id
    -- Left join: toedieningen zonder statusupdate zijn niet afgetekend en moeten blijven bestaan
    left join laatste_status
        on laatste_status.toediening_id = toedieningen.toediening_id
        and laatste_status.rn = 1
    where toedieningen.is_vrijgesteld = 0
      and overzichten.is_nep = 0

)

select * from definitief
