with medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

teamkoppelingen as (

    select * from {{ ref('stg_onsdb__team_assignments') }}

),

teams as (

    select * from {{ ref('stg_onsdb__teams') }}

),

definitief as (

    select
        medewerkers.medewerker_id,
        medewerkers.voornaam,
        medewerkers.achternaam,
        teams.team_id,
        teams.teamnaam,
        teamkoppelingen.startdatum  as teamkoppeling_startdatum,
        teamkoppelingen.einddatum   as teamkoppeling_einddatum

    from medewerkers
    inner join teamkoppelingen
        on teamkoppelingen.medewerker_id = medewerkers.medewerker_id
    inner join teams
        on teams.team_id = teamkoppelingen.team_id

)

select * from definitief
