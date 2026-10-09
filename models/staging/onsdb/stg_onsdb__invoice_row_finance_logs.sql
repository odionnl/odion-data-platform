-- Koppeltabel tussen factuurregels en de onderliggende financiële boekingen (finance_logs).

with bron as (

    select * from {{ source('ons_plan_2', 'invoice_row_finance_logs') }}

),

definitief as (

    select
        invoiceRowObjectId      as factuurregel_id,
        financeLogObjectId      as financiele_boeking_id,
        createdat               as aangemaakt_op

    from bron

)

select * from definitief
