-- Analyse: cliënten op de wachtlijst Wonen en hun huidige producten (locatieclusters)
-- Gebruikt mart_wachtlijsten + mart_clienten

select
    wl.client_id,
    wl.clientnummer,

    c.leeftijd,
    c.leeftijdsgroep,
    c.is_in_zorg,

    wl.hoofdlocatie_locatie_id,
    wl.hoofdlocatie_locatienaam,

    wl.wachtlijst_locatie_id,
    wl.wachtlijst_locatienaam,
    wl.wachtlijst_niveau3,
    wl.wachtlijst_startdatum,
    wl.wachtlijst_einddatum,

    wl.ambulant,
    wl.dagbesteding,
    wl.logeren,
    wl.wonen,
    wl.behandeling,
    wl.kind_en_gezin,
    wl.multidisciplinair_team

from {{ ref('mart_wachtlijsten') }} wl
    inner join {{ ref('mart_clienten') }} c
    on c.client_id = wl.client_id

where wl.wachtlijst_niveau3 in (N'LG (Wachtlijst)', N'VG (Wachtlijst)')

order by
    wl.wachtlijst_locatienaam,
    wl.wachtlijst_startdatum
