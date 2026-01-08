{{ config(materialized='view') }}

-- rpt_wlz_wonen_indicaties

with

-- Clientselectie: in zorg
client_in_zorg as (
    select
        client_id,
        clientnummer
    from {{ ref('dim_clienten') }}
    where in_zorg = 1
),

-- Clientselectie: alleen cliënten die bij ons wonen
woon_clienten as (
    select distinct
        client_id,
        locatie_id,
        locatienaam
    from {{ ref('fct_client_hoofdlocatie_actueel') }}
    where cluster = 'Wonen'
),

-- Selectie: cliënten met WLZ indicatie (ZZP)
wlz_actueel as (
    select distinct
        client_id,
        product_code
    from {{ ref('fct_client_zorglegitimatie_actueel') }}
    where product_financiering = 'Zorgzwaartepakket'
),

final as (
    select
        c.client_id,
        c.clientnummer,
        wc.locatie_id,
        wc.locatienaam as hoofdlocatie,
        w.product_code as indicatie
    from client_in_zorg c
    inner join woon_clienten wc
        on wc.client_id = c.client_id
    inner join wlz_actueel w
        on w.client_id = c.client_id
)

select *
from final;
