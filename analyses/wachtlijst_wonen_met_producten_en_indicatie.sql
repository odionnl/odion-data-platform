-- Analyse: cliënten op de wachtlijst Wonen met huidige producten en indicatie
-- Gebruikt mart_wachtlijsten + mart_clienten + mart_vragenlijst_antwoorden_intake_en_wachtlijst_3_0

with
    indicatie
    as
    (

        select
            client_id,
            antwoord_tekst as indicatie,
            ingevuld_op,
            row_number() over (
            partition by client_id
            order by ingevuld_op desc
        ) as rn

        from {{ ref
    
    ('mart_vragenlijst_antwoorden_intake_en_wachtlijst_3_0') }}
    where vraagtekst like 'Selecteer beschikking%'

)

select
    wl.client_id,
    wl.clientnummer,

    c.is_in_zorg,
    c.leeftijd,
    c.leeftijdsgroep2 as leeftijdsgroep,

    coalesce(i.indicatie, 'Onbekend') as indicatie,

    wl.hoofdlocatie_locatie_id,
    wl.hoofdlocatie_locatienaam,

    wl.wachtlijst_locatie_id,
    wl.wachtlijst_locatienaam,
    wl.wachtlijst_niveau3,
    l.niveau1,
    l.niveau2,
    l.niveau3,
    l.niveau4,
    wl.wachtlijst_startdatum,
    wl.wachtlijst_einddatum,
    datediff(month, wl.wachtlijst_startdatum, getdate()) as wachtduur_maanden,

    wl.ambulant,
    wl.dagbesteding,
    wl.logeren,
    wl.wonen,
    wl.behandeling,
    wl.kind_en_gezin,
    wl.multidisciplinair_team


from {{ ref
('mart_wachtlijsten_actueel') }} wl
    inner join {{ ref
('mart_clienten') }} c
        on c.client_id = wl.client_id
    left join indicatie i
        on i.client_id = wl.client_id
        and i.rn = 1
    left join {{ ref
('mart_locaties') }} l
        on l.locatie_id = wl.wachtlijst_locatie_id

where wl.wachtlijst_niveau3 in
(N'LG (Wachtlijst)', N'VG (Wachtlijst)')
    and wl.wonen = 0

order by
    wl.clientnummer
