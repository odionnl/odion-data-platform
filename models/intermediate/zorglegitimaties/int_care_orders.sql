with care_orders as (
    select *
    from {{ ref('stg_ons__care_orders') }}
),

finance_types as (
    select *
    from {{ ref('stg_ons__finance_types') }}
),

final as (
    select
        co.client_id,
        co.legitimatie_id,
        co.legitimatie_nummer,
        ft.financieringstype_code          as financiering_code,
        ft.financieringstype_beschrijving  as financiering,
        co.startdatum_legitimatie,
        co.einddatum_legitimatie
    from care_orders co
    left join finance_types ft
        on ft.financieringstype_id = co.finance_type_id
)

select *
from final;
