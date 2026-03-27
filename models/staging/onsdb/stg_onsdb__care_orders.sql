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
        beginDate               as startdatum,
        endDate                 as einddatum,
        beginDateClipped        as ingangsdatum_geldig,
        endDateClipped          as einddatum_geldig,
        teamObjectId            as team_id,
        debtorObjectId          as debiteur_id,
        financeTypeObjectId     as financieringstype_id,
        skipDeclaration         as uitsluiten_van_facturatie,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
