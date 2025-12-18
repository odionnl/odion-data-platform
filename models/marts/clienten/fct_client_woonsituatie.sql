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
    where ca.startdatum_adres < getdate()
      and (ca.einddatum_adres is null or ca.einddatum_adres > getdate())
      and ca.straatnaam is not null
      and ca.straatnaam <> ''
),

client_adres as (
    select * from client_adres_ranked where rn = 1
),

relatie_info as (
    select
        r.*,

        -- normalisatie van adres
        lower(ltrim(rtrim(r.straatnaam)))                as n_straatnaam,
        ltrim(rtrim(r.huisnummer))                       as n_huisnummer,
        lower(ltrim(rtrim(isnull(r.plaats,''))))     as n_plaats,
        lower(ltrim(rtrim(isnull(r.gemeente,''))))       as n_gemeente
    from {{ ref('int_relations') }} r
    where r.straatnaam is not null
      and r.straatnaam <> ''
      and r.startdatum_adres < getdate()
      and (r.einddatum_adres is null or r.einddatum_adres > getdate())
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
            -- woont bij Odion
            when chc.cluster = 'Wonen' then 'Woont bij Odion'
            -- vergelijken van adres met meest relevante relatie
            when
                r.n_straatnaam = ca.n_straatnaam
                and isnull(r.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                and (
                     r.n_plaats = ca.n_plaats or r.n_gemeente = ca.n_gemeente
                  or r.n_gemeente = ca.n_plaats or r.n_plaats = ca.n_gemeente
                )
            then 'Woont bij relatie'
            -- adres cliënt onbekend
            when ca.client_id is null then 'Adres cliënt onbekend'
            -- aanname bij adres relatie onbekend = zelfstandig wonen
            when (r.straatnaam is null or r.straatnaam = '') then 'Woont zelfstandig'
            -- overig: aanname = zelfstandig wonen
            else 'Woont zelfstandig'
        end as woonsituatie,

        -- Relatie-informatie
        --coalesce(r.contactpersoon_relatietype_categorie, 'onbekend') as contactpersoon_relatietype_categorie,
        coalesce(r.contactpersoon_relatietype, 'onbekend') as contactpersoon_relatietype,
        --coalesce(r.relatie_sociaal_type, 'onbekend') as relatie_sociaal_type,
        coalesce(r.persoonlijke_relatietype, 'onbekend') as persoonlijke_relatietype,
        --coalesce(r.persoonlijke_relatietype_categorie, 'onbekend') as persoonlijke_relatietype_categorie,

        -- Adresgegevens cliënt
        ca.straatnaam as client_straatnaam,
        ca.huisnummer as client_huisnummer,
        ca.plaats as client_plaats,
        ca.gemeente as client_gemeente,
        ca.adrestype  as client_adrestype,

        -- Adresgegevens relatie
        r.straatnaam as relatie_straatnaam,
        r.huisnummer as relatie_huisnummer,
        r.plaats as relatie_plaats,
        r.gemeente as relatie_gemeente

    from client_info c
    left join client_adres ca
        on ca.client_id = c.client_id
    left join client_hoofdlocatie_cluster chc
        on chc.client_id = c.client_id

    -- best match: tie break met contactpersoon_relatietype_id als proxy 
    -- (eerste contactpersoon, tweede contactpersoon, etc.)
    outer apply (
        select top (1) r.*
        from relatie_info r
        where r.client_id = c.client_id
        order by
            case
                when r.n_straatnaam = ca.n_straatnaam
                 and isnull(r.n_huisnummer,'') = isnull(ca.n_huisnummer,'')
                 and r.n_plaats = ca.n_plaats
                 and r.n_gemeente = ca.n_gemeente then 0
                when r.n_straatnaam = ca.n_straatnaam
                 and isnull(r.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 1
                when r.n_straatnaam = ca.n_straatnaam then 2
                when isnull(r.n_huisnummer,'') = isnull(ca.n_huisnummer,'') then 3
                else 9
            end,
            r.contactpersoon_relatietype_id asc
    ) r
)

select * from final;
