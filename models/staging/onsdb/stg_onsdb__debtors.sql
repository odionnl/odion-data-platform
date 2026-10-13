-- Debiteuren. Persoonsgegevens (naam, adres, contact- en bankgegevens) zijn bewust
-- niet opgenomen: een debiteur kan ook de cliënt zelf zijn.

with bron as (

    select * from {{ source('ons_plan_2', 'debtors') }}

),

definitief as (

    select
        objectId                as debiteur_id,
        debtorNumber            as debiteurnummer,
        organizationName        as debiteur_naam,
        [type]                  as debiteur_type_code,
        gemeentecode,
        clientObjectId          as client_id,
        cast(hidden as int)     as is_verborgen,
        beginDate               as startdatum,
        endDate                 as einddatum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
