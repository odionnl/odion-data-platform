{% set beoordelingsperiode_in_dagen = var('beoordelingsperiode_in_dagen', 28) %}

with clienten as (
    select 
        client_id,
        clientnummer
    from {{ ref('dim_clienten') }}
    where in_zorg = 1
),

datapunt_zorgplannen as (
    select distinct
        client_id
    from {{ ref('dim_zorgplannen') }}
    where zorgplan_status = 'Actief'
      and zorgplan_geldigheid = 'Geldig'
),

datapunt_recente_rapportage as (
    select distinct
        client_id
    from {{ ref('int_careplan_reports') }}
    where rapportage_datum >= dateadd(day, -{{ beoordelingsperiode_in_dagen }}, getdate())
)

select
    c.client_id,
    c.clientnummer,
    case when z.client_id is not null then 1 else 0 end as actueel_zorgplan,
    case when r.client_id is not null then 1 else 0 end as recente_rapportage
from clienten c
left join datapunt_zorgplannen z
    on z.client_id = c.client_id
left join datapunt_recente_rapportage r
    on r.client_id = c.client_id
