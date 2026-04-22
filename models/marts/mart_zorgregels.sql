with testclienten as (

    select * from {{ ref('int_testclienten') }}

),

zorgregels as (

    select * from {{ ref('stg_onsdb__presence_logs') }}
    where starttijd >= datefromparts(year(getdate()), 1, 1)
      and (
        client_id is null
        or client_id not in (select client_id from testclienten)
      )

),

medewerkers_in_ortec as (

    -- Medewerkers die überhaupt in ORTEC voorkomen (met minstens één dienst).
    -- Wordt gebruikt om onderscheid te maken tussen 'geen dienst op die dag'
    -- en 'medewerker heeft helemaal geen ORTEC-profiel'.
    select distinct medewerker_id
    from {{ ref('int_medewerker_rooster_dagen') }}

),

rooster_dagen as (

    select * from {{ ref('int_medewerker_rooster_dagen') }}

),

met_rooster_check as (

    select
        z.*,
        cast(z.starttijd as date)                                      as datum,
        case when mo.medewerker_id is not null then 1 else 0 end       as medewerker_in_ortec,
        case when rd.medewerker_id is not null then 1 else 0 end       as heeft_dienst_op_datum
    from zorgregels z
    left join medewerkers_in_ortec mo
        on mo.medewerker_id = z.medewerker_id
    left join rooster_dagen rd
        on rd.medewerker_id = z.medewerker_id
       and rd.datum         = cast(z.starttijd as date)

),

definitief as (

    select
        zorgregel_id,
        client_id,
        medewerker_id,
        activiteit_id,
        kostenplaats_id,

        starttijd,
        eindtijd,
        datum,
        datediff(minute, starttijd, eindtijd)       as duur_minuten,

        bron_type,
        bron_omschrijving,

        is_gefiatteerd,
        gefiatteerd_op,

        is_urenregistratie,
        is_verloning,
        heeft_tijdsduur,
        is_automatisch_verdeeld,

        -- Rooster-check: stond de medewerker die dag op het ORTEC-rooster?
        case
            when medewerker_id      is null then 'Geen medewerker'
            when medewerker_in_ortec = 0    then 'Medewerker niet in ORTEC'
            when heeft_dienst_op_datum = 1  then 'Match'
            else 'Geen dienst'
        end as rooster_match_status,
        case
            when medewerker_id       is null then null
            when medewerker_in_ortec = 0     then null
            when heeft_dienst_op_datum = 1   then 1
            else 0
        end as is_rooster_match,

        aangemaakt_op,
        gewijzigd_op

    from met_rooster_check

)

select * from definitief
