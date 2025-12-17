{{ config(materialized='view') }}

with params as (
    select cast(getdate() as date) as vandaag
),

-- 1) Alleen cliënten met een *actieve* zorgtoewijzing vandaag
clienten_in_zorg as (
    select
        c.*,
        p.vandaag
    from {{ ref('int_clients_and_care_allocations_joined') }} c
    cross join params p
    where c.startdatum_zorg <= p.vandaag
      and c.einddatum_zorg >= p.vandaag
),

-- 2) Leeftijd op vandaag
clienten_met_leeftijd as (
    select
        c.*,
        case 
            when c.geboortedatum is null then null
            else
                datediff(year, c.geboortedatum, c.vandaag)
                - case
                    when (month(c.vandaag) * 100 + day(c.vandaag))
                       < (month(c.geboortedatum) * 100 + day(c.geboortedatum))
                      then 1 else 0
                  end
        end as leeftijd
    from clienten_in_zorg c
),

-- 3) Leeftijdsgroep via je macro
clienten_met_leeftijdsgroep as (
    select
        c.*,
        {{ get_leeftijdsgroep('leeftijd') }} as leeftijdsgroep
    from clienten_met_leeftijd c
),

final as (
    select
        c.client_id,
        c.clientnummer,
        c.geboortedatum,
        c.achternaam,
        c.geboortenaam,
        c.voornaam,
        c.partnernaam,
        c.initialen,
        c.prefix,
        c.naam,
        cast(c.startdatum_zorg as date) as startdatum_zorg,
        cast(c.einddatum_zorg  as date) as einddatum_zorg,
        c.leeftijd,
        c.leeftijdsgroep
    from clienten_met_leeftijdsgroep c
)

select *
from final;
