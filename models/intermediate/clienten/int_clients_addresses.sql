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
        a.straatnaam,
        a.huisnummer,
        a.woonplaats,
        a.gemeente,
        lst_at.adrestype,

        -- bij verschillende adressen: kies meest relevante
        ROW_NUMBER() OVER (
            PARTITION BY ca.client_id
            ORDER BY
                CASE lst_at.adrestype
                    WHEN 'GBA adres (woonadres)' THEN 1
                    WHEN 'Verblijfadres' THEN 2
                    WHEN 'Tijdelijk verblijfadres' THEN 3
                    ELSE 99
                END,
                a.startdatum_adres DESC,
                a.adres_id DESC
        ) AS rn
    from clients_addresses ca
    join addresses a on a.adres_id = ca.adres_id
    join lst_address_types lst_at on lst_at.adrestype_code = a.adrestype_code
    where lst_at.adrestype in ('GBA adres (woonadres)', 'Verblijfadres', 'Tijdelijk verblijfadres')
      and a.startdatum_adres < getdate()
      and (a.einddatum_adres is null or a.einddatum_adres > getdate())
      and a.straatnaam is not null
)

select * from final
where rn = 1;
