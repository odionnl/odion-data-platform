with

source as (

    select * from {{ source('ortec', 'dim_cost_center') }}

),

renamed as (

    select
        COST_CENTER_KEY as kostenplaats_id,
        [NAME] as kostenplaats_naam,
        DESCRIPTION as kostenplaats_beschrijving,
        CODE as kostenplaats_code,
        ACTIVE_FLAG as is_actief
    from source

)

select 
    kostenplaats_id,
    kostenplaats_naam,
    kostenplaats_beschrijving,
    kostenplaats_code,
    is_actief
from renamed