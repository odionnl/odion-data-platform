{{ config(materialized='view') }}

with clients as (

    select * from {{ ref('stg_ons__clients') }}

),

care_allocations as (

    select * from {{ ref('stg_ons__care_allocations') }}

),

care_allocations_relevant as (
  select *
  from (
    select
      ca.*,
      row_number() over (
        partition by ca.client_id
        order by
          case
            when ca.startdatum_zorg <= getdate()
             and (ca.einddatum_zorg is null or ca.einddatum_zorg > getdate())
            then 1 else 0
          end desc,              -- eerst lopend
          ca.startdatum_zorg desc,
          coalesce(ca.einddatum_zorg, cast('9999-12-31' as date)) desc
      ) as rn
    from care_allocations ca
  ) x
  where rn = 1
),


clienten_met_leeftijd as (

    select 
        clients.client_id,
        datediff(year, clients.geboortedatum, getdate())
        -- correctie voor verjaardag al geweest of niet dit jaar
        - case
            when dateadd(year, datediff(year, clients.geboortedatum, getdate()), clients.geboortedatum) > getdate()
            then 1 else 0
          end as leeftijd
    from clients
),


final as (

    select
        clients.client_id,
        clients.clientnummer,
        clients.geboortedatum,
        clients.overlijdensdatum,
        clients.achternaam,
        clients.geboortenaam,
        clients.voornaam,
        clients.partnernaam,
        clients.initialen,
        clients.prefix,
        clients.naam,

        -- leeftijd & leeftijdsgroep
        clienten_met_leeftijd.leeftijd,
        {{ get_leeftijdsgroep('clienten_met_leeftijd.leeftijd') }} as leeftijdsgroep,

        -- in zorg
        cast(
            case
                when ca.startdatum_zorg <= getdate()
                and (ca.einddatum_zorg is null or ca.einddatum_zorg > getdate())
                then 1
                else 0
            end
        as bit) as in_zorg


    from clients
    left join care_allocations_relevant ca
        on ca.client_id = clients.client_id
    left join clienten_met_leeftijd
        on clients.client_id=clienten_met_leeftijd.client_id

    where clients.clientnummer is not null
      and clients.clientnummer not in ('onbekend')
      and clients.geboortedatum is not null

)

select * from final;
