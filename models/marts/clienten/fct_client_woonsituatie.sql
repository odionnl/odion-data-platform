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
        lower(ltrim(rtrim(isnull(ca.plaats,''))))     as n_plaats,
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
        lower(ltrim(rtrim(isnull(ra.plaats,''))))     as n_plaats,
        lower(ltrim(rtrim(isnull(ra.gemeente,''))))       as n_gemeente
    from {{ ref('int_relations') }} ra
    where ra.straatnaam is not null
      and ra.straatnaam <> ''
      and ra.startdatum_adres < getdate()
      and (ra.einddatum_adres is null or ra.einddatum_adres > getdate())
),

client_hoofdlocatie_cluster as (
    select
        client_id,
        locatie_id,
        locatienaam as hoofdlocatie,
        cluster
    from {{ ref('fct_client_hoofdlocatie_actueel') }}
),

final as (
    select
        c.client_id,
        c.clientnummer,
        c.in_zorg,

        -- hoofdlocatie
        chc.locatie_id,
        chc.hoofdlocatie,
        chc.cluster,

        -- Woonsituatie
        case
            when ca.client_id is null then 'Clientadres onbekend'
            when chc.cluster = 'Wonen' then 'Woont bij Odion'
            when
                ra.n_straatnaam = ca.n_straatnaam
                and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                and (
                     ra.n_plaats = ca.n_plaats or ra.n_gemeente = ca.n_gemeente
                  or ra.n_gemeente = ca.n_plaats or ra.n_plaats = ca.n_gemeente
                )
            then 'Woont bij relatie'
            else 'Woont zelfstandig'
        end as woonsituatie,

        -- Relatie-informatie
        coalesce(ra.contactpersoon_relatietype_categorie, 'onbekend') as contactpersoon_relatietype_categorie,
        coalesce(ra.relatie_type, 'onbekend') as relatie_type,
        coalesce(ra.relatie, 'onbekend') as relatie,

        -- Adresgegevens cliënt
        ca.straatnaam as client_straatnaam,
        ca.huisnummer as client_huisnummer,
        ca.plaats as client_plaats,
        ca.gemeente as client_gemeente,
        ca.adrestype  as client_adrestype,

        -- Adresgegevens relatie
        ra.straatnaam as relatie_straatnaam,
        ra.huisnummer as relatie_huisnummer,
        ra.plaats as relatie_plaats,
        ra.gemeente as relatie_gemeente

    from client_info c
    left join client_adres ca
        on ca.client_id = c.client_id
    left join client_hoofdlocatie_cluster chc
        on chc.client_id = c.client_id

    outer apply (
        select top (1) ra.*
        from relatie_adres ra
        where ra.client_id = c.client_id
        order by
            case
                when ra.n_straatnaam = ca.n_straatnaam
                 and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                 and ra.n_plaats = ca.n_plaats
                 and ra.n_gemeente = ca.n_gemeente then 0
                when ra.n_straatnaam = ca.n_straatnaam
                 and isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 1
                when ra.n_straatnaam = ca.n_straatnaam then 2
                when isnull(ra.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 3
                else 9
            end,
            ra.contactpersoon_relatietype_id asc
    ) ra
)

select * from final;
