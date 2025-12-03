{{ config(materialized='view') }}

with careplans as (

    select * from {{ ref('stg_ons__careplans') }}

),

lst_care_plan_statuses as  (

        select * from {{ ref('stg_ons__lst_care_plan_statuses') }}

),


final as (

    select
        c.zorgplan_id,
        c.client_id,
        c.werknemer_id,
        cps.zorgplan_status,
        c.startdatum_zorgplan,
        c.einddatum_zorgplan,
        c.aangemaakt_op,
        c.bewerkt_op,

        -- Geldigheidslabel
        case
            when c.startdatum_zorgplan > GETDATE() then 'Nog niet gestart'
            when c.startdatum_zorgplan <= GETDATE() and c.einddatum_zorgplan > GETDATE() then 'Geldig'
            else 'Verlopen'
        end as zorgplan_geldigheid

    from careplans c
    left join lst_care_plan_statuses cps
        on c.zorgplan_status_code=cps.zorgplan_status_code

)

select * from final;
