-- models/marts/finance/int_legitimaties.sql

with care_order_products as (
    select *
    from {{ ref('stg_ons__care_order_products') }}
),

care_orders as (
    select *
    from {{ ref('stg_ons__care_orders') }}
),

products as (
    select *
    from {{ ref('stg_ons__products') }}
),

finance_types as (
    select *
    from {{ ref('stg_ons__finance_types') }}
),

final as (

    select
        -- client details
        co.client_id,

        -- legitimatie details (finance type op care_order)
        co.legitimatie_nummer,
        ft.financieringstype_beschrijving     as legitimatie_financiering,
        co.startdatum_legitimatie,
        co.einddatum_legitimatie,

        -- product details
        p.product_code,
        p.product_omschrijving,
        pft.financieringstype_beschrijving    as product_financiering,
        cop.startdatum_legitimatie_product     as startdatum_product,
        cop.einddatum_legitimatie_product      as einddatum_product

    from care_order_products cop
    join care_orders co
        on co.legitimatie_id = cop.legitimatie_id
    join products p
        on p.product_id = cop.product_id
    left join finance_types ft
        on ft.financieringstype_id = co.finance_type_id
    left join finance_types pft
        on pft.financieringstype_id = p.financieringstype_id
)

select *
from final
