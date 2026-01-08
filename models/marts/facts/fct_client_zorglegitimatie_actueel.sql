{{ config(materialized='view') }}

with params as (
    select cast(getdate() as date) as vandaag
),

zorglegitimaties_actueel as (
    select
        *
    from {{ ref('int_care_orders') }}
    cross join params p
    where startdatum_product <= p.vandaag
      and einddatum_product > p.vandaag
),

final as (
    select
        -- grain: client x legitimatie x product (actueel)

        -- client
        client_id,

        -- legitimatie
        legitimatie_id,
        legitimatie_nummer,
        legitimatie_financiering,
        startdatum_legitimatie,
        einddatum_legitimatie,

        -- product
        product_id,
        product_code,
        product_omschrijving,
        product_financiering,
        startdatum_product,
        einddatum_product

    from zorglegitimaties_actueel
)

select *
from final;
