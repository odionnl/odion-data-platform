with source as (

    select * 
    from {{ source('ons_plan_2', 'activities') }}

),

renamed as (

    select
        objectId as uursoort_id,
        description AS uursoort_beschrijving,
        identificationNo AS uursoort_nummer
    from source

)

select
    uursoort_id,
    uursoort_beschrijving,
    uursoort_nummer
from renamed;
