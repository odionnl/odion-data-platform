with bron as (

    select * from {{ source('ons_plan_2', 'clients') }}

),

definitief as (

    select
        objectId                as client_id,
        identificationNo        as clientnummer,
        firstName               as voornaam,
        lastName                as achternaam,
        birthName               as geboortenaam,
        givenName               as roepnaam,
        partnerName             as partnernaam,
        initials                as initialen,
        prefix                  as voorvoegsel,
        concat(
            coalesce(givenName, firstName),
            case when prefix is not null then ' ' + prefix else '' end,
            ' ' + lastName
        )                       as clientnaam,
        birthNamePrefix         as voorvoegsel_geboortenaam,
        partnerNamePrefix       as voorvoegsel_partnernaam,
        dateOfBirth             as geboortedatum,
        gender                  as geslacht,
        civilStatus             as burgerlijke_staat,
        bsn,
        emailAddress            as emailadres,
        mobilePhone             as mobiel_telefoonnummer,
        hometown                as geboorteplaats,
        nationality             as nationaliteit,
        [language]              as taal,
        religion                as religie,
        deathDate               as overlijdensdatum,
        bankAccountObjectId     as bankrekening_id,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
