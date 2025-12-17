{{ config(materialized='view') }}

with params as (
    -- Stel hier je bereik in (optioneel aanpassen)
    select
        2015 as start_jaar,
        year(cast(getdate() as date)) as eind_jaar
),

-- 1) Pak uit dim_datum voor elk jaar de 1-jan-peildatum
jaren as (

    select
        d.jaar,
        min(d.datum) as peildatum
    from {{ ref('dim_datum') }} d
    join params p
      on d.jaar between p.start_jaar and p.eind_jaar
    group by d.jaar

),

clienten_met_zorgtoewijzingen as (

    select
        client_id,
        clientnummer,
        startdatum_zorg,
        einddatum_zorg
    from {{ ref('int_clients_and_care_allocations_joined') }}

),

client_jaar_in_zorg as (

    select distinct
        c.client_id,
        c.clientnummer,
        cast(c.startdatum_zorg as date) as startdatum_zorg,
        cast(c.einddatum_zorg as date) as einddatum_zorg,
        j.jaar,
        j.peildatum as peildatum
    from clienten_met_zorgtoewijzingen c
    join jaren j
      on j.peildatum between c.startdatum_zorg and c.einddatum_zorg

)

select * from client_jaar_in_zorg;