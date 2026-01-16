with

source as (

    select * from {{ source('ortec', 'dim_employee') }}

),

renamed as (

    select
        EMPLOYEE_KEY as medewerker_id,
        NAME as naam,
        EMPLOYEE_NUMBER as medewerkernummer,
        ACTIVE_FLAG as is_actief
    from source

)

select 
    medewerker_id,
    naam,
    medewerkernummer,
    is_actief
from renamed