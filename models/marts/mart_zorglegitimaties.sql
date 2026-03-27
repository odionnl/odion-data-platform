with zorglegitimaties as (

    select * from {{ ref('stg_onsdb__care_orders') }}

),

producten as (

    select * from {{ ref('stg_onsdb__care_order_products') }}

),

clienten as (

    select * from {{ ref('stg_onsdb__clients') }}

),

financieringstypen as (

    select * from {{ ref('stg_onsdb__finance_types') }}

),

product_samenvatting as (

    select
        zorglegitimatie_id,
        count(distinct zorglegitimatie_product_id)  as aantal_producten,
        sum(hoeveelheid_in_minuten)                 as totaal_minuten

    from producten
    where zorglegitimatie_product_id is not null
    group by zorglegitimatie_id

),

definitief as (

    select
        zorglegitimaties.zorglegitimatie_id,
        zorglegitimaties.zorglegitimatie_type,
        zorglegitimaties.legitimatienummer,
        zorglegitimaties.startdatum,
        zorglegitimaties.einddatum,
        zorglegitimaties.ingangsdatum_geldig,
        zorglegitimaties.einddatum_geldig,

        -- Client
        zorglegitimaties.client_id,
        clienten.clientnaam                                         as client_naam,

        -- Financiering
        zorglegitimaties.financieringstype_id,
        financieringstypen.financieringstype_naam,
        financieringstypen.financieringsgroep,

        -- Debiteur
        zorglegitimaties.debiteur_id,

        -- Facturatie
        zorglegitimaties.uitsluiten_van_facturatie,

        -- Producten (geaggregeerd)
        coalesce(product_samenvatting.aantal_producten, 0)          as aantal_producten,
        coalesce(product_samenvatting.totaal_minuten, 0)            as totaal_minuten,

        -- Status
        case
            when zorglegitimaties.startdatum <= cast(getdate() as date)
                and (zorglegitimaties.einddatum is null or zorglegitimaties.einddatum > cast(getdate() as date))
            then 1
            else 0
        end                                                         as is_actief,

        -- Timestamps
        zorglegitimaties.aangemaakt_op,
        zorglegitimaties.gewijzigd_op

    from zorglegitimaties
    left join clienten
        on clienten.client_id = zorglegitimaties.client_id
    left join financieringstypen
        on financieringstypen.financieringstype_id = zorglegitimaties.financieringstype_id
    left join product_samenvatting
        on product_samenvatting.zorglegitimatie_id = zorglegitimaties.zorglegitimatie_id

)

select * from definitief
