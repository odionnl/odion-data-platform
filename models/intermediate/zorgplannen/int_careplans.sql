{{ config(materialized='view') }}

with careplans as (

    select * from {{ ref('stg_ons__careplans') }}

),

lst_care_plan_statuses as  (

        select * from {{ ref('stg_ons__lst_care_plan_statuses') }}

),

careplan_entries_agg as (

    SELECT
        ce.zorgplan_id,
        MIN(CASE WHEN ce.domein_versie IN ('Nieuw','Oud') THEN ce.domein_versie END) AS minv,
        MAX(CASE WHEN ce.domein_versie IN ('Nieuw','Oud') THEN ce.domein_versie END) AS maxv,
        COUNT(CASE WHEN ce.domein_versie IN ('Nieuw','Oud') THEN 1 END)                AS cnt_valid
    FROM {{ ref('int_careplan_entries') }} AS ce
    GROUP BY
    ce.zorgplan_id

),


final as (

    select
        careplans.zorgplan_id,
        careplans.client_id,
        careplans.werknemer_id,
        lst_care_plan_statuses.zorgplan_status,
        careplans.startdatum_zorgplan,
        careplans.einddatum_zorgplan,
        careplans.aangemaakt_op,
        careplans.bewerkt_op,

        -- Geldigheidslabel
        case
            when careplans.startdatum_zorgplan > GETDATE() then 'Nog niet gestart'
            when careplans.startdatum_zorgplan <= GETDATE() and careplans.einddatum_zorgplan > GETDATE() then 'Geldig'
            else 'Verlopen'
        end as zorgplan_geldigheid,

        -- Versie: Oud/Nieuw/Leeg/Mix (gebaseerd op domeinen)
        case
            when careplan_entries_agg.zorgplan_id is null or careplan_entries_agg.cnt_valid = 0 then 'Leeg'    -- geen (geldige) regels
            when careplan_entries_agg.minv = careplan_entries_agg.maxv then careplan_entries_agg.minv               -- allemaal Nieuw of allemaal Oud
            else 'Mix'                                                                                              -- combinatie
        end as zorgplan_versie

    from careplans
    left join lst_care_plan_statuses
        on careplans.zorgplan_status_code=lst_care_plan_statuses.zorgplan_status_code
    left join careplan_entries_agg
        on careplans.zorgplan_id=careplan_entries_agg.zorgplan_id

)

select * from final;
