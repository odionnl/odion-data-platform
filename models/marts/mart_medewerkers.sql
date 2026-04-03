with medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

teams as (

    -- Meest recent team per medewerker (voorkeur voor actieve koppeling)
    select
        medewerker_id,
        team_id,
        teamnaam,
        teamkoppeling_startdatum,
        teamkoppeling_einddatum,
        row_number() over (
            partition by medewerker_id
            order by
                case
                    when teamkoppeling_einddatum is null
                      or teamkoppeling_einddatum >= cast(getdate() as date)
                    then 0 else 1
                end,
                teamkoppeling_startdatum desc
        ) as rn

    from {{ ref('int_medewerkers_met_teams') }}

),

contracten as (

    -- Meest recent contract per medewerker (voorkeur voor actief contract)
    select
        medewerker_id,
        contract_id,
        contracttype_naam,
        contract_startdatum,
        contract_einddatum,
        normtijd_uren_per_week,
        variabele_uren_per_week,
        row_number() over (
            partition by medewerker_id
            order by
                case
                    when contract_einddatum is null
                      or contract_einddatum >= cast(getdate() as date)
                    then 0 else 1
                end,
                contract_startdatum desc
        ) as rn

    from {{ ref('int_medewerkers_met_contracten') }}

),

definitief as (

    select
        medewerkers.medewerker_id,
        medewerkers.personeelsnummer,
        medewerkers.voornaam,
        medewerkers.achternaam,
        medewerkers.geboortedatum,
        medewerkers.emailadres,
        medewerkers.mobiel_telefoonnummer,
        medewerkers.is_onderaannemer,

        -- Actief vlag (1 = heeft actief contract vandaag)
        case
            when contracten.contract_einddatum is null
              or contracten.contract_einddatum >= cast(getdate() as date)
            then 1 else 0
        end as is_actief,

        -- Team (meest recent, bij voorkeur actief)
        teams.teamnaam,

        -- Contract (meest recent, bij voorkeur actief)
        contracten.contracttype_naam                    as contracttype,
        contracten.contract_startdatum,
        contracten.contract_einddatum,
        contracten.normtijd_uren_per_week,
        contracten.variabele_uren_per_week,

        medewerkers.aangemaakt_op,
        medewerkers.gewijzigd_op

    from medewerkers
    left join teams
        on teams.medewerker_id = medewerkers.medewerker_id
        and teams.rn = 1
    left join contracten
        on contracten.medewerker_id = medewerkers.medewerker_id
        and contracten.rn = 1

)

select * from definitief
