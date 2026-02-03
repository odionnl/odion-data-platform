with source as (

    select * 
    from {{ source('ons_plan_2', 'teams') }}

),

renamed as (

    select
        objectId as team_id,
        name as team_naam,
        identificationNo as kostenplaatsnummer,
        name as kostenplaats_naam,
        poolcluster as pool_type_code
    from source

)

select
    team_id,
    team_naam,
    kostenplaatsnummer,
    kostenplaats_naam,
    pool_type_code
from renamed;
