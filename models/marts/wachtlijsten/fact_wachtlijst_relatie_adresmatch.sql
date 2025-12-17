{{ config(materialized='view') }}

with client_wachtlijst as (

    select * from {{ ref('fact_client_wachtlijsten_actueel') }}

),

client_hoofdlocatie as (

    select * from {{ ref('fact_client_hoofdlocatie_actueel') }}

),

client_info as (

    select * from {{ ref('int_clients') }}

),

client_adres as (

    select 
    *,

    -- normalisatie van adres
    lower(ltrim(rtrim(client_straatnaam)))                   AS n_straat,
    ltrim(rtrim(client_huisnummer))                      AS n_huisnr,
    lower(ltrim(rtrim(isnull(client_woonplaats,''))))          AS n_plaats,
    lower(ltrim(rtrim(isnull(client_gemeente,''))))  AS n_gemeente

    from {{ ref('int_clients_addresses') }}

),

relatie_detail as (

    select 
    *,

    -- normalisatie van adres
    lower(ltrim(rtrim(relatie_straatnaam)))                   AS n_straat,
    ltrim(rtrim(relatie_huisnummer))                      AS n_huisnr,
    lower(ltrim(rtrim(isnull(relatie_woonplaats,''))))          AS n_plaats,
    lower(ltrim(rtrim(isnull(relatie_gemeente,''))))  AS n_gemeente
    from {{ ref('int_relations') }}

),

final as (

    select distinct
        c.client_id,
        c.clientnummer,
        c.geboortedatum,
        c.leeftijd,
        c.in_zorg,

        ch.locatienaam as hoofdlocatie,
        ch.locatie_id as locatie_id_hoofdlocatie,

        cw.locatienaam as wachtlijst,
        cw.locatie_id as locatie_id_wachtlijst,

        case
            when rd.n_straat = ca.n_straat
             and isnull(rd.n_huisnr,'') = isnull(ca.n_huisnr,'')
             and (
                  rd.n_plaats = ca.n_plaats or rd.n_gemeente = ca.n_gemeente
               or rd.n_gemeente = ca.n_plaats or rd.n_plaats = ca.n_gemeente
             )
            then 1 else 0
        end as match_adres_relatie,

        coalesce(rd.relatie_categorie, 'onbekend') as relatie_categorie,
        coalesce(rd.relatie_type, 'onbekend')      as relatie_type,
        coalesce(rd.relatie, 'onbekend')           as relatie,

        ca.client_straatnaam,
        ca.client_huisnummer,
        ca.client_woonplaats,
        ca.client_gemeente,

        rd.relatie_straatnaam,
        rd.relatie_huisnummer,
        rd.relatie_woonplaats,
        rd.relatie_gemeente

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
                when rd.n_straat = ca.n_straat
                 and isnull(rd.n_huisnr,'') = isnull(ca.n_huisnr,'')
                 and rd.n_plaats = ca.n_plaats
                 and rd.n_gemeente = ca.n_gemeente then 0

                when rd.n_straat = ca.n_straat
                 and isnull(rd.n_huisnr,'') = isnull(ca.n_huisnr,'') then 1

                when rd.n_straat = ca.n_straat then 2
                when isnull(rd.n_huisnr,'') = isnull(ca.n_huisnr,'') then 3
                else 9
            end,
            rd.relatie_volgorde asc
    ) rd

    where c.leeftijd >= 18

)

select * from final;
