with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

zorgplannen as (

    select * from {{ ref('stg_onsdb__careplans') }}

),

statussen as (

    select * from {{ ref('stg_onsdb__lst_care_plan_statuses') }}

),

actieve_zorgplannen as (

    select distinct zorgplannen.client_id
    from zorgplannen
    inner join statussen
        on statussen.status_code = zorgplannen.status_code
    where statussen.status_omschrijving = 'Actief'
      and zorgplannen.startdatum <= cast(getdate() as date)
      and (zorgplannen.einddatum is null or zorgplannen.einddatum > cast(getdate() as date))

),

definitief as (

    select
        clienten.client_id,
        case
            when actieve_zorgplannen.client_id is not null then 1
            else 0
        end as geldig_zorgplan

    from clienten
    left join actieve_zorgplannen
        on actieve_zorgplannen.client_id = clienten.client_id

)

select * from definitief
