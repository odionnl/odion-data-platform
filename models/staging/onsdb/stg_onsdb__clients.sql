with bron as (

    select * from {{ source('ons_plan_2', 'clients') }}

),

geslachten as (

    select * from {{ ref('stg_onsdb__lst_genders') }}

),

definitief as (

    select
        bron.objectId                as client_id,
        bron.identificationNo        as clientnummer,
        bron.firstName               as voornaam,
        bron.lastName                as achternaam,
        bron.birthName               as geboortenaam,
        bron.givenName               as roepnaam,
        bron.partnerName             as partnernaam,
        bron.initials                as initialen,
        bron.prefix                  as voorvoegsel,
        LTRIM(RTRIM(REPLACE(
            concat(
                coalesce(bron.givenName, bron.firstName),
                case when bron.prefix is not null then ' ' + bron.prefix else '' end,
                ' ' + bron.lastName
            ),
            '  ', ' '
        )))                     as clientnaam,
        bron.birthNamePrefix         as voorvoegsel_geboortenaam,
        bron.partnerNamePrefix       as voorvoegsel_partnernaam,
        bron.dateOfBirth             as geboortedatum,
        geslachten.geslacht,
        bron.civilStatus             as burgerlijke_staat,
        bron.bsn,
        bron.emailAddress            as emailadres,
        bron.mobilePhone             as mobiel_telefoonnummer,
        bron.hometown                as geboorteplaats,
        bron.nationality             as nationaliteit,
        bron.[language]              as taal,
        bron.religion                as religie,
        bron.deathDate               as overlijdensdatum,
        bron.bankAccountObjectId     as bankrekening_id,
        bron.createdAt               as aangemaakt_op,
        bron.updatedAt               as gewijzigd_op

    from bron
    left join geslachten
        on geslachten.geslacht_code = bron.gender

)

select * from definitief
