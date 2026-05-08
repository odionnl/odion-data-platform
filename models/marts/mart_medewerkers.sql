with medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

teams as (

    -- Meest recent team per medewerker (voorkeur voor actieve koppeling)
    select
        medewerker_id,
        team_id,
        teamnaam,
        startdatum_teamkoppeling,
        einddatum_teamkoppeling,
        row_number() over (
            partition by medewerker_id
            order by
                case
                    when einddatum_teamkoppeling is null
                      or einddatum_teamkoppeling >= cast(getdate() as date)
                    then 0 else 1
                end,
                startdatum_teamkoppeling desc
        ) as rn

    from {{ ref('int_medewerkers_met_teams') }}

),

contracten as (

    -- Meest recent contract per medewerker (voorkeur voor actief contract)
    select
        medewerker_id,
        contract_id,
        contracttype_naam,
        startdatum_contract,
        einddatum_contract,
        normtijd_uren_per_week,
        variabele_uren_per_week,
        row_number() over (
            partition by medewerker_id
            order by
                case
                    when einddatum_contract is null
                      or einddatum_contract >= cast(getdate() as date)
                    then 0 else 1
                end,
                startdatum_contract desc
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
            when contracten.einddatum_contract is null
              or contracten.einddatum_contract >= cast(getdate() as date)
            then 1 else 0
        end as is_actief,

        -- Team (meest recent, bij voorkeur actief)
        teams.teamnaam,

        -- Contract (meest recent, bij voorkeur actief)
        contracten.contracttype_naam                    as contracttype,
        contracten.startdatum_contract,
        contracten.einddatum_contract,
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
