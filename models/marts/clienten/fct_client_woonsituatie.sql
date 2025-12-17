{{ config(materialized='view') }}

with

client_info as (
    select * from {{ ref('int_clients') }}
),

client_adres_ranked as (
    select
        ca.*,

        -- normalisatie van adres
        lower(ltrim(rtrim(ca.straatnaam)))                as n_straatnaam,
        ltrim(rtrim(ca.huisnummer))                       as n_huisnummer,
        lower(ltrim(rtrim(isnull(ca.woonplaats,''))))     as n_woonplaats,
        lower(ltrim(rtrim(isnull(ca.gemeente,''))))       as n_gemeente,

        -- bij verschillende adressen: kies meest relevante
        row_number() over (
            partition by ca.client_id
            order by
                case ca.adrestype
                    when 'GBA adres (woonadres)' then 1
                    when 'Verblijfadres' then 2
                    when 'Tijdelijk verblijfadres' then 3
                    else 99
                end,
                ca.startdatum_adres desc,
                ca.adres_id desc
        ) as rn
    from {{ ref('int_clients_addresses') }} ca
    where ca.adrestype in ('GBA adres (woonadres)', 'Verblijfadres', 'Tijdelijk verblijfadres')
      and ca.startdatum_adres < getdate()
      and (ca.einddatum_adres is null or ca.einddatum_adres > getdate())
      and ca.straatnaam is not null
      and ca.straatnaam <> ''
),

client_adres as (
    select * from client_adres_ranked where rn = 1
),

relatie_adres as (
    select
        ra.*,

        -- normalisatie van adres
        lower(ltrim(rtrim(ra.straatnaam)))                as n_straatnaam,
        ltrim(rtrim(ra.huisnummer))                       as n_huisnummer,
        lower(ltrim(rtrim(isnull(ra.woonplaats,''))))     as n_woonplaats,
        lower(ltrim(rtrim(isnull(ra.gemeente,''))))       as n_gemeente
    from {{ ref('int_relations') }} ra
    where ra.straatnaam is not null
      and ra.straatnaam <> ''
      and ra.startdatum_adres < getdate()
      and (ra.einddatum_adres is null or ra.einddatum_adres > getdate())
),

final as (
    select
        c.client_id,
        c.clientnummer,
        c.in_zorg,

        -- Woonsituatie
        case
            when ca.client_id is null then 'Clientadres onbekend'
            when
                ra.n_straatnaam = ca.n_straatnaam
                and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                and (
                     ra.n_woonplaats = ca.n_woonplaats or ra.n_gemeente = ca.n_gemeente
                  or ra.n_gemeente = ca.n_woonplaats or ra.n_woonplaats = ca.n_gemeente
                )
            then 'Woont bij relatie'
            else 'Woont op zichzelf'
        end as woonsituatie,

        -- Relatie-informatie
        coalesce(ra.relatie_type_categorie, 'onbekend') as relatie_type_categorie,
        coalesce(ra.relatie_type, 'onbekend') as relatie_type,
        coalesce(ra.relatie, 'onbekend') as relatie,

        -- Adresgegevens cliënt
        ca.straatnaam as client_straatnaam,
        ca.huisnummer as client_huisnummer,
        ca.woonplaats as client_woonplaats,
        ca.gemeente as client_gemeente,
        ca.adrestype  as client_adrestype,

        -- Adresgegevens relatie
        ra.straatnaam as relatie_straatnaam,
        ra.huisnummer as relatie_huisnummer,
        ra.woonplaats as relatie_woonplaats,
        ra.gemeente as relatie_gemeente

    from client_info c
    left join client_adres ca
        on ca.client_id = c.client_id

    outer apply (
        select top (1) ra.*
        from relatie_adres ra
        where ra.client_id = c.client_id
        order by
            case
                when ra.n_straatnaam = ca.n_straatnaam
                 and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                 and ra.n_woonplaats = ca.n_woonplaats
                 and ra.n_gemeente = ca.n_gemeente then 0
                when ra.n_straatnaam = ca.n_straatnaam
                 and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 1
                when ra.n_straatnaam = ca.n_straatnaam then 2
                when isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 3
                else 9
            end,
            ra.client_contact_relatie_type_id asc
    ) ra
)

select * from final;
