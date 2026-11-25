-- Ons Agenda-afspraken op reeksniveau: één rij per afspraak (eenmalig of herhalende reeks).
-- Dimensie voor mart_agenda_afspraak_momenten en mart_agenda_deelnames (join op afspraak_id).
-- All-time (geen venster), met is_actief berekend op GETDATE().
-- Uitzonderingsrijen uit Ons (is_uitzondering = 1) zijn geen afspraken en staan hier niet in;
-- ze worden geteld in aantal_uitzonderingen.
-- AVG: titel is leeg bij privé-afspraken; de opmerking wordt niet opgenomen. Geen namen, alleen ID's.
-- Afspraken waarbij een testcliënt (int_testclienten) is uitgenodigd zijn uitgesloten.
-- Grain: één rij per afspraak_id.

with afspraken_bron as (

    select * from {{ ref('stg_onsdb__agenda_events') }}

),

afspraken as (

    select * from afspraken_bron
    where is_uitzondering = 0

),

uitzonderingen as (

    select reeks_id, cast(geldig_vanaf as date) as datum
    from afspraken_bron
    where is_uitzondering = 1

),

genodigden as (

    select * from {{ ref('int_agenda_genodigden') }}

),

testclienten as (

    select * from {{ ref('int_testclienten') }}

),

afspraken_met_testclient as (

    select distinct g.afspraak_id
    from genodigden g
    inner join testclienten t
        on t.client_id = g.client_id

),

uursoorten as (

    select * from {{ ref('stg_onsdb__agenda_hour_types') }}

),

activiteiten as (

    select * from {{ ref('stg_onsdb__activities') }}

),

agenda_medewerkers as (

    select * from {{ ref('stg_onsdb__onsagenda_employees') }}

),

labels_per_afspraak as (

    select
        al.afspraak_id,
        string_agg(l.labelnaam, ' | ') within group (order by l.prioriteit, l.labelnaam) as labels

    from {{ ref('stg_onsdb__agenda_event_labels') }} al
    inner join {{ ref('stg_onsdb__agenda_labels') }} l
        on l.label_id = al.label_id

    group by al.afspraak_id

),

aantallen_per_afspraak as (

    select
        afspraak_id,
        count(distinct medewerker_id)   as aantal_medewerkers,
        count(distinct client_id)       as aantal_clienten,
        count(*)                        as aantal_genodigden

    from genodigden
    group by afspraak_id

),

basis as (

    select
        a.*,
        lower(ltrim(rtrim(coalesce(a.herhalingstype, 'single')))) as herhaling,
        case
            when lower(ltrim(rtrim(coalesce(a.herhalingstype, 'single'))))
                 in ('daily', 'weekly', 'monthly_on_day', 'monthly_on_week', 'yearly') then 1
            else 0
        end as is_herhalend

    from afspraken a

),

uitzonderingen_per_afspraak as (

    -- Uitzonderingen horen bij de reeks (reeks_id); tel alleen die binnen de looptijd van deze afspraak
    select
        b.afspraak_id,
        count(*) as aantal_uitzonderingen

    from basis b
    inner join uitzonderingen u
        on u.reeks_id = b.reeks_id
        and u.datum >= cast(b.geldig_vanaf as date)
        and (b.geldig_tot is null or u.datum <= cast(b.geldig_tot as date))

    where b.is_herhalend = 1
    group by b.afspraak_id

),

definitief as (

    select
        -- Sleutels
        b.afspraak_id,
        b.reeks_id,

        -- Afspraak
        case when b.is_prive = 1 then null else b.titel end as titel,
        b.is_prive,
        b.is_client_aanwezig,
        b.locatie_omschrijving,
        b.bron_applicatie,

        -- Tijd
        dateadd(second, cast(coalesce(b.starttijd_seconden, 0) as int), cast('00:00' as time(0)))
                                                            as starttijd,
        dateadd(
            second,
            cast(coalesce(b.starttijd_seconden, 0) + coalesce(b.duur_seconden, 0) as int),
            cast('00:00' as time(0))
        )                                                   as eindtijd,
        cast(coalesce(b.duur_seconden, 0) / 60.0 as decimal(10, 2))
                                                            as duur_minuten,

        -- Herhaling
        b.herhalingstype,
        case b.herhaling
            when 'single'          then 'Eenmalig'
            when 'daily'           then 'Dagelijks'
            when 'weekly'          then 'Wekelijks'
            when 'monthly_on_day'  then 'Maandelijks (vaste dag)'
            when 'monthly_on_week' then 'Maandelijks (vaste week)'
            when 'yearly'          then 'Jaarlijks'
            else 'Onbekend'
        end                                                 as herhaling_omschrijving,
        b.is_herhalend,
        cast(b.geldig_vanaf as date)                        as startdatum,
        case
            when b.is_herhalend = 1 then cast(b.geldig_tot as date)
            else cast(b.geldig_vanaf as date)
        end                                                 as einddatum,
        case when b.is_herhalend = 1 and b.geldig_tot is null then 1 else 0 end
                                                            as is_zonder_einddatum,
        case when b.is_herhalend = 1 then b.herhaling_interval end
                                                            as herhaling_interval,
        case when b.herhaling = 'weekly' then
            nullif(concat_ws(', ',
                case when b.is_maandag   = 1 then 'ma' end,
                case when b.is_dinsdag   = 1 then 'di' end,
                case when b.is_woensdag  = 1 then 'wo' end,
                case when b.is_donderdag = 1 then 'do' end,
                case when b.is_vrijdag   = 1 then 'vr' end,
                case when b.is_zaterdag  = 1 then 'za' end,
                case when b.is_zondag    = 1 then 'zo' end
            ), '')
        end                                                 as weekdagen,
        coalesce(upa.aantal_uitzonderingen, 0)              as aantal_uitzonderingen,

        -- Nog niet afgelopen (lopend of in de toekomst)
        case
            when b.is_herhalend = 1
                 and (b.geldig_tot is null or cast(b.geldig_tot as date) >= cast(getdate() as date)) then 1
            when b.is_herhalend = 0
                 and cast(b.geldig_vanaf as date) >= cast(getdate() as date) then 1
            else 0
        end                                                 as is_actief,

        -- Uursoort
        us.activiteit_id,
        act.beschrijving                                    as uursoort_beschrijving,

        -- Labels
        lpa.labels,

        -- Genodigden
        coalesce(apa.aantal_medewerkers, 0)                 as aantal_medewerkers,
        coalesce(apa.aantal_clienten, 0)                    as aantal_clienten,
        coalesce(apa.aantal_genodigden, 0)                  as aantal_genodigden,

        -- Maker en tijdstempels
        am.medewerker_id                                    as aangemaakt_door_medewerker_id,
        b.aangemaakt_op,
        b.gewijzigd_op

    from basis b
    left join uursoorten us
        on us.agenda_uursoort_id = b.agenda_uursoort_id
    left join activiteiten act
        on act.activiteit_id = us.activiteit_id
    left join labels_per_afspraak lpa
        on lpa.afspraak_id = b.afspraak_id
    left join aantallen_per_afspraak apa
        on apa.afspraak_id = b.afspraak_id
    left join uitzonderingen_per_afspraak upa
        on upa.afspraak_id = b.afspraak_id
    left join agenda_medewerkers am
        on am.agenda_medewerker_id = b.aangemaakt_door_id

    where b.afspraak_id not in (select afspraak_id from afspraken_met_testclient)

)

select * from definitief
