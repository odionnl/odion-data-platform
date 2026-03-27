with bron as (

    select * from {{ source('ons_plan_2', 'addresses') }}

),

definitief as (

    select
        objectId                as adres_id,
        street                  as straatnaam,
        homeNumber              as huisnummer,
        homeNumberExtension     as huisnummer_toevoeging,
        roomNumber              as kamernummer,
        city                    as plaatsnaam,
        zipcode                 as postcode,
        municipality            as gemeentenaam,
        country                 as land,
        telephoneNumber         as telefoonnummer,
        email                   as emailadres,
        residentType            as adrestype_code,
        latitude,
        longitude,
        locationDescription     as locatieomschrijving,
        situationDescription    as toelichting_woonsituatie,
        beginDate               as startdatum,
        endDate                 as einddatum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
