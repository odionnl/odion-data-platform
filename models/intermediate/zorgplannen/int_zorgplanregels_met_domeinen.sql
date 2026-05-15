-- Classificeert elke zorgplanregel op domein_versie (Oud/Nieuw/Onbekend).
-- Slank gehouden: alleen wat int_zorgplannen_met_versie nodig heeft voor de
-- versie-aggregatie per zorgplan. Voor brede regel-info, zie mart_zorgplanregels.

with zorgplanregels as (

    select
        zorgplanregel_id,
        zorgplan_id,
        doeldefinitie_id
    from {{ ref('stg_onsdb__careplan_entries') }}

),

doeldefinities as (

    select
        doeldefinitie_id,
        domein_definitie_id
    from {{ ref('stg_onsdb__careplan_target_definitions') }}

),

domeindefinities as (

    select
        domein_definitie_id,
        is_oud_domein
    from {{ ref('stg_onsdb__careplan_domain_definitions') }}

)

select
    zorgplanregels.zorgplanregel_id,
    zorgplanregels.zorgplan_id,
    case
        when domeindefinities.is_oud_domein = 1 then 'Oud'
        when domeindefinities.is_oud_domein = 0 then 'Nieuw'
        else 'Onbekend'
    end as domein_versie

from zorgplanregels
left join doeldefinities
    on doeldefinities.doeldefinitie_id = zorgplanregels.doeldefinitie_id
left join domeindefinities
    on domeindefinities.domein_definitie_id = doeldefinities.domein_definitie_id
