-- Analyse: cliënten op de wachtlijst Kind en Gezin (historisch) met wachtduur in maanden
-- Gebruikt mart_wachtlijsten + mart_clienten + stg_onsdb__care_allocations

with zorgtoewijzingen as (

    -- Een cliënt kan meerdere zorgtoewijzingen hebben (perioden in zorg).
    -- startdatum_in_zorg = vroegste startdatum.
    -- einddatum_in_zorg  = laatste einddatum, of NULL als er nog een open periode loopt.
    select
        client_id,
        min(startdatum) as startdatum_in_zorg,
        case
            when count(*) > count(einddatum) then null
            else max(einddatum)
        end as einddatum_in_zorg
    from {{ ref('stg_onsdb__care_allocations') }}
    group by client_id

)

select
    wl.client_id,
    wl.clientnummer,

    wl.is_actief as wachtlijst_is_actief,

    c.is_in_zorg,
    za.startdatum_in_zorg,
    za.einddatum_in_zorg,

    wl.wachtlijst_locatie_id,
    wl.wachtlijst_locatienaam,
    wl.wachtlijst_niveau3,
    l.niveau1,
    l.niveau2,
    l.niveau3,
    l.niveau4,
    wl.startdatum_wachtlijst,
    wl.einddatum_wachtlijst,
    datediff(
        month,
        wl.startdatum_wachtlijst,
        coalesce(wl.einddatum_wachtlijst, cast(getdate() as date))
    ) as wachtduur_maanden

from {{ ref('mart_wachtlijsten') }} wl
inner join {{ ref('mart_clienten') }} c
    on c.client_id = wl.client_id
left join {{ ref('mart_locaties') }} l
    on l.locatie_id = wl.wachtlijst_locatie_id
left join zorgtoewijzingen za
    on za.client_id = wl.client_id

where wl.wachtlijst_niveau3 = N'Kind en Gezin (Wachtlijst)'

order by wl.clientnummer
