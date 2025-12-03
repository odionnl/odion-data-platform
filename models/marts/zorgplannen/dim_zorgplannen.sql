with src as (

    select * from {{ ref('int_careplans') }}

)

select
    src.zorgplan_id,
    src.client_id,
    src.werknemer_id,
    src.zorgplan_status,
    src.startdatum_zorgplan,
    src.einddatum_zorgplan,
    src.aangemaakt_op,
    src.bewerkt_op,
    src.zorgplan_geldigheid
from src;
