{{ config(materialized='view') }}

with clienten_in_zorg_per_jaar as (

    select *
    from {{ ref('fact_client_in_zorg_per_jaar') }}

),

clients as (

    select
        client_id,
        geboortedatum
    from {{ ref('int_clients') }}

),

joined as (

    select
        f.client_id,
        f.clientnummer,
        f.jaar,
        f.peildatum,
        c.geboortedatum
    from clienten_in_zorg_per_jaar f
    join clients c
      on f.client_id = c.client_id

),

clienten_met_leeftijd as (

    select
        j.*,
        datediff(year, j.geboortedatum, j.peildatum)
        - case
            when (month(j.peildatum) * 100 + day(j.peildatum))
               < (month(j.geboortedatum) * 100 + day(j.geboortedatum))
              then 1 else 0
          end as leeftijd
    from joined j

),

clienten_met_leeftijdsgroep as (

    select
        l.*,
        {{ get_leeftijdsgroep('leeftijd') }} as leeftijdsgroep
    from clienten_met_leeftijd l

)

select *
from clienten_met_leeftijdsgroep;
