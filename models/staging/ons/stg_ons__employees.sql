with source as (

    select * 
    from {{ source('ons_plan_2', 'employees') }}

),

renamed as (

    select
        objectId as medewerker_id,
        identificationNo AS medewerker_nummer
    from source

)

select
    medewerker_id,
    medewerker_nummer
from renamed;
