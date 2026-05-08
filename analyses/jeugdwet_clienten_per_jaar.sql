-- Aantal unieke cliënten per jaar (vanaf 2021) met een Jeugdwet-zorglegitimatie.
--
-- Definitie: een cliënt telt mee in jaar X als hij/zij in dat jaar ten minste
-- één dag een Jeugdwet-legitimatie had, dus wanneer de legitimatie-periode
-- [startdatum, einddatum] overlapt met [jaar-01-01, jaar-12-31].
-- Legitimaties zonder einddatum worden als nog-lopend beschouwd.
--
-- Extra filter: cliënt was jonger dan 18 op de startdatum van de legitimatie
-- (ten tijde van de zorglegitimatie). Cliënten zonder geboortedatum worden
-- uitgesloten.

with legitimaties as (

    select
        legit.client_id,
        legit.startdatum,
        legit.einddatum
    from {{ ref('mart_zorglegitimaties') }} as legit
    inner join {{ ref('stg_onsdb__clients') }} as cli
        on cli.client_id = legit.client_id
    where legit.financieringstype_naam = 'Jeugdwet'
      and cli.geboortedatum is not null
      and legit.startdatum < dateadd(year, 18, cli.geboortedatum)

),

jaren as (

    select 2021 as jaar
    union all select 2022
    union all select 2023
    union all select 2024
    union all select 2025
    union all select 2026

),

overlap as (

    select
        jaren.jaar,
        legitimaties.client_id
    from jaren
    inner join legitimaties
        on legitimaties.startdatum <= datefromparts(jaren.jaar, 12, 31)
       and (
                legitimaties.einddatum is null
             or legitimaties.einddatum >= datefromparts(jaren.jaar, 1, 1)
           )
    where jaren.jaar <= year(cast(getdate() as date))

)

select
    jaar,
    count(distinct client_id) as aantal_unieke_clienten
from overlap
group by jaar
order by jaar
