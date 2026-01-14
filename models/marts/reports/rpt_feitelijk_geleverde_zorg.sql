with clienten as (
    select 
        client_id,
        clientnummer
    from {{ ref('dim_clienten') }}
    where in_zorg = 1
),

datapunt_zorgplannen as (
    select
        *
    from {{ ref('dim_zorgplannen') }}
    where zorgplan_status = 'Actief'
        and zorgplan_geldigheid = 'Geldig'
)

select
    clienten.client_id,
    clienten.clientnummer,
    case
        when zorgplannen.zorgplan_id is not null then 1
        else 0
    end as actueel_zorgplan
from clienten
    left join zorgplannen
    on zorgplannen.client_id=clienten.client_id