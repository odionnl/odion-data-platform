with clienten_adressen as (

    select * from {{ ref('stg_onsdb__clients_addresses') }}

),

adressen as (

    select * from {{ ref('stg_onsdb__addresses') }}

),

adrestypen as (

    select * from {{ ref('stg_onsdb__lst_address_types') }}

),

definitief as (

    select
        ca.client_id,
        a.adres_id,
        at.adrestype,
        a.straatnaam,
        a.huisnummer,
        a.huisnummer_toevoeging,
        a.plaatsnaam,
        a.postcode,
        a.gemeentenaam,
        a.startdatum,
        a.einddatum

    from clienten_adressen ca
    join adressen a
        on a.adres_id = ca.adres_id
    left join adrestypen at
        on at.adrestype_code = a.adrestype_code

)

select * from definitief
