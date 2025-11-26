{{ config(materialized='view') }}

with clienten as (

    select
        client_id,
        clientnummer,
        geboortedatum,
        overlijdensdatum,
        achternaam,
        geboortenaam,
        voornaam,
        partnernaam,
        initialen,
        prefix,
        naam
    from {{ ref('stg_ons__clients') }}

),

clienten_met_basis as (

    select
        c.*,

        -- is de cliënt overleden?
        case 
            when overlijdensdatum is not null then 1
            else 0
        end as is_overleden,

        year(geboortedatum)  as geboortejaar,
        month(geboortedatum) as geboortemaand,

        -- leeftijd op vandaag (met correctie maand/dag)
        case 
            when geboortedatum is null then null
            else
                datediff(year, geboortedatum, cast(getdate() as date))
                - case
                    when (month(cast(getdate() as date)) * 100 + day(cast(getdate() as date)))
                       < (month(geboortedatum) * 100 + day(geboortedatum))
                      then 1 else 0
                  end
        end as leeftijd_vandaag
    from clienten c

),

zorgtoewijzingen as (

    select
        client_id,
        cast(startdatum_zorg as date) as startdatum_zorg,
        cast(einddatum_zorg  as date) as einddatum_zorg
    from {{ ref('stg_ons__care_allocations') }}

),

zorgstatus_per_client as (

    select
        z.client_id,

        -- 1 als er *minstens één* lopende zorgtoewijzing is op vandaag
        max(
            case
                when startdatum_zorg <= cast(getdate() as date)
                 and (einddatum_zorg is null or einddatum_zorg >= cast(getdate() as date))
                then 1 else 0
            end
        ) as is_in_zorg_vandaag
    from zorgtoewijzingen z
    group by z.client_id

),

samengevoegd as (

    select
        c.*,
        coalesce(z.is_in_zorg_vandaag, 0) as is_in_zorg_vandaag
    from clienten_met_basis c
    left join zorgstatus_per_client z
        on z.client_id = c.client_id
)

select *
from samengevoegd;
