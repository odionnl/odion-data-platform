with bron as (

    select * from {{ source('ons_plan_2', 'invoices') }}

),

definitief as (

    select
        objectId                        as factuur_id,
        invoiceNumber                   as factuurnummer,
        [description]                   as factuur_omschrijving,
        invoiceType                     as factuur_type_code,
        debtorObjectId                  as debiteur_id,
        exportProfileObjectId           as exportprofiel_id,
        provider                        as zorgaanbieder_code,
        [date]                          as factuurdatum,
        beginDate                       as startdatum,
        endDate                         as einddatum,
        bookdate                        as boekdatum,
        cast(completed as int)          as is_geexporteerd,
        cast(rejected as int)           as is_afgekeurd,
        returnProcessedAt               as retour_verwerkt_op,
        returnMessage                   as retourmelding,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
