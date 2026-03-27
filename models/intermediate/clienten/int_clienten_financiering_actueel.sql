-- Primaire financieringsvorm per client op basis van actieve zorglegitimaties (vandaag).
-- Bij meerdere financieringstypes wint de hoogste prioriteit (ZZP > WMO > Jeugdwet > PGB > Onderaanneming).
-- Grain: één rij per client.

with clienten as (

    select client_id
    from {{ ref('stg_onsdb__clients') }}

),

actieve_zorglegitimaties as (

    select
        client_id,
        financieringstype_id
    from {{ ref('stg_onsdb__care_orders') }}
    where startdatum <= cast(getdate() as date)
      and (einddatum is null or einddatum > cast(getdate() as date))

),

financieringstypes as (

    select
        financieringstype_id,
        financieringstype_naam
    from {{ ref('stg_onsdb__finance_types') }}

),

zorg_met_financiering as (

    select
        az.client_id,
        coalesce(ft.financieringstype_naam, 'Onbekend') as financieringstype_naam,
        row_number() over (
            partition by az.client_id
            order by
                case coalesce(ft.financieringstype_naam, '')
                    when 'Zorgzwaartepakket' then 1
                    when 'Wet maatschappelijke ondersteuning' then 2
                    when 'Jeugdwet' then 3
                    when 'PGB' then 4
                    when 'Onderaanneming' then 5
                    else 999
                end
        ) as rn
    from actieve_zorglegitimaties az
    left join financieringstypes ft
        on ft.financieringstype_id = az.financieringstype_id

),

definitief as (

    select
        c.client_id,
        coalesce(zmf.financieringstype_naam, 'Geen actieve zorglegitimatie') as financiering

    from clienten c
    left join zorg_met_financiering zmf
        on zmf.client_id = c.client_id
        and zmf.rn = 1

)

select * from definitief
