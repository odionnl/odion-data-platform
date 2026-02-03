with source as (

    select * 
    from {{ source('ons_plan_2', 'presence_logs') }}

),

renamed as (

    select
        objectId as zorgregel_id,
        clientId as client_id,
        employeeId as medewerker_id,
        costClusterObjectId as team_id,
        activityObjectId as uursoort_id,
        startDate as startdatum,
        endDate as einddatum,
        removed as is_verwijderd,
        registration as is_urenregistratie,
        payment as is_voor_verloning,
        verified as is_gefiatteerd,
        verifiedDate as fiatteringsdatum
    from source

)

select
    zorgregel_id,
    client_id,
    medewerker_id,
    team_id,
    uursoort_id,
    startdatum,
    einddatum,
    is_verwijderd,
    is_urenregistratie,
    is_voor_verloning,
    is_gefiatteerd,
    fiatteringsdatum
from renamed;
