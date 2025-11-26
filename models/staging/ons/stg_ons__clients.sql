with

source as (

    select * from {{ source('ons_plan_2', 'clients') }}

),

renamed as (

    select
        objectId as client_id,
        identificationNo as clientnummer,
        cast(dateOfBirth as date) as geboortedatum,
        cast(deathDate as date) as overlijdensdatum,
        lastName as achternaam,
        birthName as geboortenaam,
        givenName as voornaam,
        partnerName as partnernaam,
        initials as initialen,
        prefix as prefix,
        [name] as naam
    from source

)

select 
    client_id,
    clientnummer,
    geboortedatum,
    overlijdensdatum,
    achternaam,
    geboortenaam,
    voornaam,
    partnernaam,
    initialen,
    prefix,
    naam
from renamed