with zorgregels as (

    -- Alleen nog-niet-gefiatteerde zorgregels — de RPA-check is bedoeld om
    -- openstaande regels te screenen vóór fiattering.
    select * from {{ ref('mart_zorgregels') }}
    where is_gefiatteerd = 0

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

met_checks as (

    select
        z.*,
        case when mo.medewerker_id is not null then 1 else 0 end   as medewerker_in_ortec,
        case when rd.medewerker_id is not null then 1 else 0 end   as heeft_dienst_op_datum,
        rd.heeft_werk_tijd
    from zorgregels z
    left join medewerkers_in_ortec mo
        on mo.medewerker_id = z.medewerker_id
    left join rooster_dagen rd
        on rd.medewerker_id = z.medewerker_id
       and rd.datum         = z.datum

),

definitief as (

    select
        zorgregel_id,
        client_id,
        clientnummer,
        medewerker_id,
        personeelsnummer,
        medewerker_naam,
        activiteit_id,
        uursoort_beschrijving,
        team_id,
        teamnaam,

        starttijd,
        eindtijd,
        datum,
        duur_minuten,

        is_urenregistratie,
        is_gefiatteerd,
        gefiatteerd_op,

        -- Rooster-check: stond de medewerker die dag op het ORTEC-rooster?
        case
            when medewerker_id         is null then 'Geen medewerker'
            when medewerker_in_ortec    = 0    then 'Medewerker niet in ORTEC'
            when heeft_dienst_op_datum  = 1    then 'Match'
            else 'Geen dienst'
        end as rooster_match_beschrijving,

        -- Gecombineerde check: 1 als de medewerker op die dag een ORTEC-dienst
        -- had mét TIME_AT_WORK > 0 (echte werkdag), anders 0. Dus ook 0 bij
        -- ziek/verlof/standby, geen dienst, geen ORTEC-profiel of geen medewerker.
        case
            when heeft_werk_tijd = 1 then 1
            else 0
        end as is_rooster_match,

        aangemaakt_op,
        gewijzigd_op

    from met_checks

)

select * from definitief
