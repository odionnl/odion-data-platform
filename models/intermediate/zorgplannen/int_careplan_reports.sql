{{ config(materialized='view') }}

with careplan_reports as (

    select * from {{ ref('stg_ons__careplan_reports') }}

),

careplan_report_types as  (

        select * from {{ ref('stg_ons__careplan_report_types') }}

),

lst_care_plan_report_types as  (

        select * from {{ ref('stg_ons__lst_care_plan_report_types') }}

),


final as (

    select
        careplan_reports.rapportage_id,
        careplan_reports.zorgplanregel_id,
        careplan_reports.client_id,
        careplan_reports.werknemer_id,
        careplan_reports.rapportage_datum,
        careplan_reports.rapportage_type_id,
        careplan_reports.opmerking,
        careplan_report_types.rapportage_type_naam,
        lst_care_plan_report_types.rapportage_type
    from careplan_reports
    left join careplan_report_types
        on careplan_report_types.rapportage_type_id=careplan_reports.rapportage_type_id
    left join lst_care_plan_report_types
        on lst_care_plan_report_types.rapportage_type_code=careplan_report_types.rapportage_type_code

)

select * from final;
