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

definitief as (

    select
        toedieningen.toediening_id,
        overzichten.client_id,
        toedienafspraken.medication_chart_id,
        overzichten.gegenereerd_op      as overzicht_gegenereerd_op,
        overzichten.datum               as overzicht_datum,
        toedieningen.ingepland_op,
        statusupdates.status,
        statusupdates.aangemaakt_op     as status_gewijzigd_op

    from toedienafspraken
    inner join toedieningen
        on toedieningen.toedienafspraak_id = toedienafspraken.toedienafspraak_id
    inner join statusupdates
        on statusupdates.toediening_id = toedieningen.toediening_id
    inner join overzichten
        on overzichten.medication_chart_id = toedienafspraken.medication_chart_id
    where toedieningen.is_vrijgesteld = 0
      and overzichten.is_nep = 0

)

select * from definitief
