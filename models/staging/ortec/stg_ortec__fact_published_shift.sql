with

source as (

    select * from {{ source('ortec', 'fact_published_shift') }}

),

renamed as (

    select
        EMPLOYEE_KEY as medewerker_id,
        BEGIN_DATE_KEY as startdatum_id,
        END_DATE_KEY as einddatum_id,
        START_TIME_KEY as starttijd_id,
        END_TIME_KEY as eindtijd_id,
        DEPARTMENT_KEY as afdeling_id,
        COST_CENTER_KEY as kostenplaats_id,
        FUNCTION_KEY as functie_id,
        SHIFT_ID as dienst_id,
        [NAME] as naam,
        DURATION as duur,
        ROSTER_STATUS as roosterstatus,
        DATE_CREATED as aangemaakt_op
    from source

)

select 
    medewerker_id,
    startdatum_id,
    einddatum_id,
    starttijd_id,
    eindtijd_id,
    afdeling_id,
    kostenplaats_id,
    functie_id,
    dienst_id,
    naam,
    duur,
    roosterstatus,
    aangemaakt_op
from renamed