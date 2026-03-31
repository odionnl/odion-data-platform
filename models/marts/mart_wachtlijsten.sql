with actieve_koppelingen as (

    select
        lk.client_id,
        lk.locatie_id,
        lk.startdatum,
        lk.einddatum,
        lk.type_toekenning,
        l.locatienaam,
        l.cluster,
        l.niveau3

    from {{ ref('stg_onsdb__location_assignments') }} lk
        inner join {{ ref('int_locatie_hierarchie') }} l
        on l.locatie_id = lk.locatie_id

),

wachtlijst_clienten as (

    select
        client_id,
        locatie_id    as wachtlijst_locatie_id,
        locatienaam   as wachtlijst_locatienaam,
        niveau3       as wachtlijst_niveau3,
        startdatum    as wachtlijst_startdatum,
        einddatum     as wachtlijst_einddatum

    from actieve_koppelingen
    where cluster = N'Wachtlijsten'
        and (einddatum is null or einddatum >= cast(getdate() as date))

),

hoofdlocaties as (

    select
        client_id,
        locatie_id    as hoofdlocatie_locatie_id,
        locatienaam   as hoofdlocatie_locatienaam,
        row_number() over (
            partition by client_id
            order by startdatum desc
        ) as rn

    from actieve_koppelingen
    where type_toekenning = N'MAIN'
        and (einddatum is null or einddatum >= cast(getdate() as date))

),

definitief as (

    select
        c.client_id,
        c.clientnummer,

        hl.hoofdlocatie_locatie_id,
        hl.hoofdlocatie_locatienaam,

        wl.wachtlijst_locatie_id,
        wl.wachtlijst_locatienaam,
        wl.wachtlijst_niveau3,
        wl.wachtlijst_startdatum,
        wl.wachtlijst_einddatum,

        -- Huidige producten (1/0 per locatiecluster)
        max(case when ak.cluster = N'Ambulant'               then 1 else 0 end) as ambulant,
        max(case when ak.cluster = N'Dagbesteding'            then 1 else 0 end) as dagbesteding,
        max(case when ak.cluster = N'Logeren'                 then 1 else 0 end) as logeren,
        max(case when ak.cluster = N'Wonen'                   then 1 else 0 end) as wonen,
        max(case when ak.cluster = N'Behandeling'             then 1 else 0 end) as behandeling,
        max(case when ak.cluster = N'Kind en gezin'           then 1 else 0 end) as kind_en_gezin,
        max(case when ak.cluster = N'Multidisciplinair Team'  then 1 else 0 end) as multidisciplinair_team

    from wachtlijst_clienten wl
        inner join {{ ref('stg_onsdb__clients') }} c
        on c.client_id = wl.client_id
        left join hoofdlocaties hl
        on hl.client_id = wl.client_id
            and hl.rn = 1
        left join actieve_koppelingen ak
        on ak.client_id = wl.client_id
            and ak.cluster <> N'Wachtlijsten'
            and (ak.einddatum is null or ak.einddatum >= cast(getdate() as date))

    group by
        c.client_id,
        c.clientnummer,
        hl.hoofdlocatie_locatie_id,
        hl.hoofdlocatie_locatienaam,
        wl.wachtlijst_locatie_id,
        wl.wachtlijst_locatienaam,
        wl.wachtlijst_niveau3,
        wl.wachtlijst_startdatum,
        wl.wachtlijst_einddatum

)

select * from definitief
