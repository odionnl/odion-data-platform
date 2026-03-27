with producten as (

    select * from {{ ref('stg_onsdb__care_order_products') }}

),

zorglegitimaties as (

    select * from {{ ref('stg_onsdb__care_orders') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

financieringstypen as (

    select * from {{ ref('stg_onsdb__finance_types') }}

),

definitief as (

    select
        producten.zorglegitimatie_product_id,
        producten.zorglegitimatie_id,
        producten.product_id,

        -- Product details
        producten.hoeveelheid_in_minuten,
        producten.startdatum,
        producten.einddatum,
        producten.ingangsdatum_geldig,
        producten.einddatum_geldig,
        producten.awbz_klasse_id,

        -- Legitimatie-context
        zorglegitimaties.zorglegitimatie_type,
        zorglegitimaties.legitimatienummer,
        zorglegitimaties.startdatum                                 as legitimatie_startdatum,
        zorglegitimaties.einddatum                                  as legitimatie_einddatum,

        -- Client
        zorglegitimaties.client_id,
        clienten.clientnaam                                         as client_naam,

        -- Financiering
        financieringstypen.financieringstype_naam,

        -- Status
        case
            when producten.startdatum <= cast(getdate() as date)
                and (producten.einddatum is null or producten.einddatum > cast(getdate() as date))
            then 1
            else 0
        end                                                         as is_actief,

        -- Timestamps
        producten.aangemaakt_op,
        producten.gewijzigd_op

    from producten
    inner join zorglegitimaties
        on zorglegitimaties.zorglegitimatie_id = producten.zorglegitimatie_id
    left join clienten
        on clienten.client_id = zorglegitimaties.client_id
    left join financieringstypen
        on financieringstypen.financieringstype_id = zorglegitimaties.financieringstype_id

)

select * from definitief
