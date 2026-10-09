-- Factuurregels. Bedragen en tarieven staan in de bron in centen.

with bron as (

    select * from {{ source('ons_plan_2', 'invoice_rows') }}

),

definitief as (

    select
        objectId                        as factuurregel_id,
        invoiceObjectId                 as factuur_id,
        previousInvoiceRowObjectId      as vorige_factuurregel_id,
        clientObjectId                  as client_id,
        productObjectId                 as product_id,
        careOrderProductObjectId        as zorglegitimatie_product_id,
        careOrderProductType            as zorglegitimatie_product_type,
        debtorObjectId                  as debiteur_id,
        provider                        as zorgaanbieder_code,
        referenceNumber                 as referentienummer,
        beginDate                       as startdatum,
        endDate                         as einddatum,
        amount                          as aantal,
        unit                            as eenheid_code,
        tarief                          as tarief_in_centen,
        tariefUnit                      as tarief_eenheid_code,
        vat                             as btw_tarief,
        vatAmount                       as btw_bedrag_in_centen,
        totalCostAmount                 as totaalbedrag_in_centen,
        calculatedAmount                as berekend_bedrag_in_centen,
        acceptedAmount                  as toegekend_bedrag_in_centen,
        cast(accepted as int)           as is_goedgekeurd,
        reasonRejected                  as retourmelding,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
