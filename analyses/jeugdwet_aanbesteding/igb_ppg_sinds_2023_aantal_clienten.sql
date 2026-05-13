-- Analyse: aantal unieke cliënten met een IGB- of PPG-productregel die op enig moment sinds 2023
-- liep (einddatum vanaf 2023-01-01, of nog open). Uitsplitsing IGB vs PPG plus totaal.

with regels as (

    select
        zlp.client_id,
        case
            when zlp.product_omschrijving like '%IGB%' then 'IGB'
            when zlp.product_omschrijving like '%PPG%' then 'PPG'
        end as product_groep
    from {{ ref('mart_zorglegitimatie_producten') }} zlp
    where (zlp.product_omschrijving like '%PPG%' or zlp.product_omschrijving like '%IGB%')
      and (zlp.einddatum >= '2023-01-01' or zlp.einddatum is null)

),

per_groep as (

    select
        product_groep,
        count(distinct client_id) as aantal_clienten
    from regels
    group by product_groep

),

totaal as (

    select
        'Totaal (IGB of PPG)' as product_groep,
        count(distinct client_id) as aantal_clienten
    from regels

)

select * from per_groep
union all
select * from totaal
order by product_groep
