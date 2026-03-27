with bron as (

    select * from {{ source('ons_plan_2', 'employees') }}

),

definitief as (

    select
        objectId                as medewerker_id,
        identificationNo        as personeelsnummer,
        firstName               as voornaam,
        lastName                as achternaam,
        birthName               as geboortenaam,
        partnerName             as partnernaam,
        initials                as initialen,
        prefix                  as voorvoegsel,
        birthNamePrefix         as voorvoegsel_geboortenaam,
        partnerNamePrefix       as voorvoegsel_partnernaam,
        dateOfBirth             as geboortedatum,
        gender                  as geslacht,
        contractId              as contractnummer,
        mobilePhone             as mobiel_telefoonnummer,
        emailAddress            as emailadres,
        homeEmailAddress        as prive_emailadres,
        profileObjectId         as weekkaartprofiel_id,
        bankAccountObjectId     as bankrekening_id,
        isSubContractor         as is_onderaannemer,
        verifiedUntilDate       as gefiatteerd_tot_datum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
