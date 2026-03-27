with medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

huidige_teams as (

    select
        medewerker_id,
        team_id,
        teamnaam,
        teamkoppeling_startdatum,
        row_number() over (
            partition by medewerker_id
            order by teamkoppeling_startdatum desc
        ) as rn

    from {{ ref('int_medewerkers_met_teams') }}
    where teamkoppeling_einddatum is null
       or teamkoppeling_einddatum >= getdate()

),

huidige_contracten as (

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
            order by contract_startdatum desc
        ) as rn

    from {{ ref('int_medewerkers_met_contracten') }}
    where contract_einddatum is null
       or contract_einddatum >= getdate()

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

        -- Huidig team
        huidige_teams.teamnaam                          as huidig_teamnaam,

        -- Huidig contract
        huidige_contracten.contracttype_naam            as huidig_contracttype,
        huidige_contracten.contract_startdatum          as huidig_contract_startdatum,
        huidige_contracten.contract_einddatum           as huidig_contract_einddatum,
        huidige_contracten.normtijd_uren_per_week       as huidig_normtijd_uren_per_week,
        huidige_contracten.variabele_uren_per_week      as huidig_variabele_uren_per_week,

        medewerkers.aangemaakt_op,
        medewerkers.gewijzigd_op

    from medewerkers
    left join huidige_teams
        on huidige_teams.medewerker_id = medewerkers.medewerker_id
        and huidige_teams.rn = 1
    left join huidige_contracten
        on huidige_contracten.medewerker_id = medewerkers.medewerker_id
        and huidige_contracten.rn = 1

)

select * from definitief
