{{ config(materialized='view') }}

with clients_addresses as (
    select * from {{ ref('stg_ons__clients_addresses') }}
),

addresses as (
    select * from {{ ref('stg_ons__addresses') }}
),

lst_address_types as (
    select * from {{ ref('stg_ons__lst_address_types') }}
),

final as (
    select
        ca.client_id,
        a.adres_id,
        a.straatnaam,
        a.huisnummer,
        a.plaats,
        a.gemeente,
        a.startdatum_adres,
        a.einddatum_adres,
        lst_at.adrestype
    from clients_addresses ca
    join addresses a on a.adres_id = ca.adres_id
    join lst_address_types lst_at on lst_at.adrestype_code = a.adrestype_code

)

select * from final;
