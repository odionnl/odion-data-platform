with medewerkers as (

    select * from {{ ref('stg_onsdb__employees') }}

),

contracten as (

    select * from {{ ref('stg_onsdb__contracts') }}

),

contracttypes as (

    select * from {{ ref('stg_onsdb__contract_types') }}

),

definitief as (

    select
        medewerkers.medewerker_id,
        medewerkers.voornaam,
        medewerkers.achternaam,
        contracten.contract_id,
        contracttypes.contracttype_naam,
        contracten.startdatum           as contract_startdatum,
        contracten.einddatum            as contract_einddatum,
        contracten.normtijd_uren_per_week,
        contracten.variabele_uren_per_week

    from medewerkers
    inner join contracten
        on contracten.medewerker_id = medewerkers.medewerker_id
    left join contracttypes
        on contracttypes.contracttype_id = contracten.contracttype_id

)

select * from definitief
