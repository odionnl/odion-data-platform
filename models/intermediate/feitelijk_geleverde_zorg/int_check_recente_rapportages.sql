with clienten as (

    select * from {{ ref('int_clienten_in_zorg_actueel') }}

),

rapportages as (

    select * from {{ ref('stg_onsdb__careplan_reports') }}

),

rapportages_in_periode as (

    select distinct client_id
    from rapportages
    where rapportagedatum >= dateadd(day, -{{ var('evaluatieperiode_dagen') }}, cast(getdate() as date))
      and rapportagedatum <= cast(getdate() as date)

),

definitief as (

    select
        clienten.client_id,
        case
            when rapportages_in_periode.client_id is not null then 1
            else 0
        end as recente_rapportages

    from clienten
    left join rapportages_in_periode
        on rapportages_in_periode.client_id = clienten.client_id

)

select * from definitief
