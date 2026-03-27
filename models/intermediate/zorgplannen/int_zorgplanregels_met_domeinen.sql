with zorgplanregels as (

    select * from {{ ref('stg_onsdb__careplan_entries') }}

),

doeldefinities as (

    select * from {{ ref('stg_onsdb__careplan_target_definitions') }}

),

domeindefinities as (

    select * from {{ ref('stg_onsdb__careplan_domain_definitions') }}

),

definitief as (

    select
        zorgplanregels.zorgplanregel_id,
        zorgplanregels.zorgplan_id,
        zorgplanregels.zorgplanregel_link_id,
        zorgplanregels.doeldefinitie_id,
        domeindefinities.domein_definitie_id,
        domeindefinities.domein_naam,
        doeldefinities.doel_naam,
        zorgplanregels.doel_titel,
        zorgplanregels.doel_opmerking,
        zorgplanregels.streefdatum,
        zorgplanregels.percentage_gerealiseerd,
        case
            when domeindefinities.is_oud_domein = 1 then 'Oud'
            when domeindefinities.is_oud_domein = 0 then 'Nieuw'
            else 'Onbekend'
        end                                     as domein_versie,
        zorgplanregels.aangemaakt_op,
        zorgplanregels.gewijzigd_op

    from zorgplanregels
    left join doeldefinities
        on doeldefinities.doeldefinitie_id = zorgplanregels.doeldefinitie_id
    left join domeindefinities
        on domeindefinities.domein_definitie_id = doeldefinities.domein_definitie_id

)

select * from definitief
