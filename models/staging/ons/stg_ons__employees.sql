with source as (

    select * 
    from {{ source('ons_plan_2', 'employees') }}

),

renamed as (

    select
        objectId as medewerker_id,
        identificationNo AS mederwerker_nummer
    from source

)

select
    medewerker_id,
    mederwerker_nummer
from renamed;
