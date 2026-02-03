with source as (

    select * 
    from {{ source('ons_plan_2', 'presence_logs') }}

),

renamed as (

    select
        objectId as zorgregel_id,
        clientId AS client_id,
        startDate AS startdatum,
        endDate AS einddatum,
        removed AS is_verwijderd,
        registration AS is_urenregistratie,
        payment AS is_voor_verloning,
        verified AS is_gefiatteerd,
        verifiedDate AS fiatteringsdatum
    from source

)

select
    zorgregel_id,
    client_id,
    startdatum,
    einddatum,
    is_verwijderd,
    is_urenregistratie,
    is_voor_verloning,
    is_gefiatteerd,
    fiatteringsdatum
from renamed;
