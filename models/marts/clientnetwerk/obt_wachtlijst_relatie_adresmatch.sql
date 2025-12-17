{{ config(materialized='view') }}

with client_wachtlijst as (

    select * from {{ ref('fct_client_wachtlijsten_actueel') }}

),

client_hoofdlocatie as (

    select * from {{ ref('fct_client_hoofdlocatie_actueel') }}

),

client_info as (

    select * from {{ ref('int_clients') }}

),

client_adres as (

    select 
    *,

    -- normalisatie van adres
    lower(ltrim(rtrim(straatnaam)))                   AS n_straatnaam,
    ltrim(rtrim(huisnummer))                      AS n_huisnummer,
    lower(ltrim(rtrim(isnull(woonplaats,''))))          AS n_woonplaats,
    lower(ltrim(rtrim(isnull(gemeente,''))))  AS n_gemeente

    from {{ ref('int_clients_addresses') }}

),

relatie_detail as (

    select 
    *,

    -- normalisatie van adres
    lower(ltrim(rtrim(straatnaam)))                   AS n_straatnaam,
    ltrim(rtrim(huisnummer))                      AS n_huisnummer,
    lower(ltrim(rtrim(isnull(woonplaats,''))))          AS n_woonplaats,
    lower(ltrim(rtrim(isnull(gemeente,''))))  AS n_gemeente
    from {{ ref('int_relations') }}

    where straatnaam is not null
        and straatnaam != ''
        and startdatum_adres < getdate()
        and (einddatum_adres is null or einddatum_adres > getdate())

),

final as (

    select distinct
        c.client_id,
        c.clientnummer,
        c.geboortedatum,
        c.leeftijd,
        c.in_zorg,

        -- hoofdlocatie
        ch.locatienaam as hoofdlocatie,
        ch.locatie_id as locatie_id_hoofdlocatie,

        -- wachtlijst
        cw.locatienaam as wachtlijst,
        cw.locatie_id as locatie_id_wachtlijst,

        case
            when rd.n_straatnaam = ca.n_straatnaam
             and isnull(rd.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
             and (
                  rd.n_woonplaats = ca.n_woonplaats or rd.n_gemeente = ca.n_gemeente
               or rd.n_gemeente = ca.n_woonplaats or rd.n_woonplaats = ca.n_gemeente
             )
            then 1 else 0
        end as match_adres_relatie,

        -- relatie-gegevens
        coalesce(rd.relatie_type_categorie, 'onbekend') as relatie_type_categorie,
        coalesce(rd.relatie_type, 'onbekend')      as relatie_type,
        coalesce(rd.relatie, 'onbekend')           as relatie,

        -- client adres
        ca.straatnaam as client_straatnaam,
        ca.huisnummer as client_huisnummer,
        ca.woonplaats as client_woonplaats,
        ca.gemeente as client_gemeente,

        -- relatie adres
        rd.straatnaam as relatie_straatnaam,
        rd.huisnummer as relatie_huisnummer,
        rd.woonplaats as relatie_woonplaats,
        rd.gemeente as relatie_gemeente

    from client_wachtlijst cw
    left join client_info c
        on c.client_id = cw.client_id
    left join client_adres ca
        on ca.client_id = cw.client_id
    left join client_hoofdlocatie ch
        on ch.client_id = cw.client_id

    outer apply (
        select top (1)
            rd.*
        from relatie_detail rd
        where rd.client_id = cw.client_id
        order by
            case
                when rd.n_straatnaam = ca.n_straatnaam
                 and isnull(rd.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                 and rd.n_woonplaats = ca.n_woonplaats
                 and rd.n_gemeente = ca.n_gemeente then 0

                when rd.n_straatnaam = ca.n_straatnaam
                 and isnull(rd.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 1

                when rd.n_straatnaam = ca.n_straatnaam then 2
                when isnull(rd.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 3
                else 9
            end,
            -- "volgorde" proxy
            rd.client_contact_relatie_type_id asc
    ) rd

    where c.leeftijd >= 18

)

select * from final;
