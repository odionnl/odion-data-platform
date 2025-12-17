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

clienten as (

    select
        client_id,
        clientnummer
    from {{ ref('int_clients') }}

),

zorgtoewijzingen as (

    select
        client_id,
        startdatum_zorg,
        einddatum_zorg
    from {{ ref('int_care_allocations') }}
),

client_jaar_in_zorg as (

    select distinct
        c.client_id,
        c.clientnummer,
        cast(z.startdatum_zorg as date) as startdatum_zorg,
        cast(z.einddatum_zorg as date) as einddatum_zorg,
        j.jaar,
        j.peildatum as peildatum
    from clienten c
    left join zorgtoewijzingen z on c.client_id=z.client_id
    join jaren j
      on j.peildatum between z.startdatum_zorg and z.einddatum_zorg

)

select * from client_jaar_in_zorg;