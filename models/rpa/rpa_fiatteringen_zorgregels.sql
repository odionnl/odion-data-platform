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
        is_werktijd,
        is_direct,
        is_reistijd,
        team_id,
        teamnaam,

        starttijd,
        eindtijd,
        datum,
        duur_minuten,

        is_gefiatteerd,
        gefiatteerd_op,

        is_urenregistratie,
        is_verloning,
        heeft_tijdsduur,
        is_automatisch_verdeeld,

        -- Rooster-check: stond de medewerker die dag op het ORTEC-rooster?
        case
            when medewerker_id         is null then 'Geen medewerker'
            when medewerker_in_ortec    = 0    then 'Medewerker niet in ORTEC'
            when heeft_dienst_op_datum  = 1    then 'Match'
            else 'Geen dienst'
        end as rooster_match_status,

        -- Gecombineerde check: had de medewerker op die dag een ORTEC-dienst
        -- met daadwerkelijk werk-tijd (TIME_AT_WORK > 0)? Dus: echte werkdag,
        -- geen pure ziek/verlof/standby. NULL als niet te beoordelen
        -- (geen medewerker of geen ORTEC-profiel).
        case
            when medewerker_id         is null then null
            when medewerker_in_ortec    = 0    then null
            when heeft_dienst_op_datum  = 0    then 0
            when heeft_werk_tijd        = 1    then 1
            else 0
        end as is_tijdens_werkdag,

        aangemaakt_op,
        gewijzigd_op

    from met_checks

)

select * from definitief
