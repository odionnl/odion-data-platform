with bron as (

    select * from {{ source('ons_plan_2', 'care_orders') }}

),

definitief as (

    select
        objectId                as zorglegitimatie_id,
        clientObjectId          as client_id,
        careOrderType           as zorglegitimatie_type,
        id                      as legitimatienummer,
        clientId                as clientnummer,
        beginDateClipped        as startdatum,
        endDateClipped          as einddatum,
        beginDate               as startdatum_origineel,
        endDate                 as einddatum_origineel,
        teamObjectId            as team_id,
        debtorObjectId          as debiteur_id,
        financeTypeObjectId     as financieringstype_id,
        skipDeclaration         as uitsluiten_van_facturatie,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
