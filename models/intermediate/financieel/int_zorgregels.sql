with zorgregels as (
    select * from {{ ref('stg_ons__presence_logs') }}
),

medewerkers as (
    select * from {{ ref('stg_ons__employees') }}
),

teams as (
    select * from {{ ref('stg_ons__teams') }}
),

lst_pool_types as (
    select * from {{ ref('stg_ons__lst_pool_types')}}
),

uursoorten as (
    select * from {{ ref('stg_ons__activities') }}
),

final as(

    select top(100)
        zorgregels.startdatum,
        zorgregels.einddatum,
        medewerkers.medewerker_nummer,
        --TRIM(CONCAT(e.lastName, ', ', e.initials, ' ', e.prefix)) AS employee_name,
        uursoorten.uursoort_beschrijving,
        uursoorten.uursoort_nummer,
        teams.kostenplaatsnummer,
        teams.kostenplaats_naam,
        lst_pool_types.pool_type,
        zorgregels.client_id,
        zorgregels.is_verwijderd,
        zorgregels.is_urenregistratie,
        zorgregels.is_voor_verloning,
        zorgregels.is_gefiatteerd,
        zorgregels.fiatteringsdatum
    from zorgregels
        left join medewerkers
        on medewerkers.medewerker_id=zorgregels.medewerker_id
        left join teams
        on teams.team_id=zorgregels.team_id
        left join lst_pool_types
        on lst_pool_types.pool_type_code=teams.pool_type_code
        left join uursoorten
        on uursoorten.uursoort_id=zorgregels.uursoort_id
)

select * from final;