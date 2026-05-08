with zorglegitimaties as (

    select * from {{ ref('stg_onsdb__care_orders') }}

),

zorglegitimatieproducten as (

    select * from {{ ref('stg_onsdb__care_order_products') }}

),

definitief as (

    select
        zorglegitimaties.zorglegitimatie_id,
        zorglegitimaties.client_id,
        zorglegitimaties.zorglegitimatie_type,
        zorglegitimaties.legitimatienummer,
        zorglegitimaties.startdatum             as startdatum_legitimatie,
        zorglegitimaties.einddatum              as einddatum_legitimatie,
        zorglegitimaties.team_id,
        zorglegitimatieproducten.zorglegitimatie_product_id,
        zorglegitimatieproducten.product_id,
        zorglegitimatieproducten.hoeveelheid_in_minuten,
        zorglegitimatieproducten.startdatum     as startdatum_product,
        zorglegitimatieproducten.einddatum      as einddatum_product

    from zorglegitimaties
    left join zorglegitimatieproducten
        on zorglegitimatieproducten.zorglegitimatie_id = zorglegitimaties.zorglegitimatie_id

)

select * from definitief
