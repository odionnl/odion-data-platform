{{ config(materialized='view') }}

with careplan_entries as (

    select * from {{ ref('stg_ons__careplan_entries') }}

),

careplan_target_definitions as (

    select * from {{ ref('stg_ons__careplan_target_definitions') }}

),

careplan_demand_definitions as (

    select * from {{ ref('stg_ons__careplan_demand_definitions') }}

),


careplan_domain_definitions as (

    select * from {{ ref('stg_ons__careplan_domain_definitions') }}

),


joined as (

    select
        careplan_entries.*,
        careplan_target_definitions.doel_naam,
        careplan_demand_definitions.aandachtspunt_naam,
        careplan_domain_definitions.domein_naam
    from careplan_entries
    left join careplan_target_definitions
        on careplan_target_definitions.doel_definitie_id=careplan_entries.doel_definitie_id
    left join careplan_demand_definitions
        on careplan_demand_definitions.aandachtspunt_id=careplan_entries.aandachtspunt_id
    left join careplan_domain_definitions
        on careplan_domain_definitions.domein_id = careplan_target_definitions.domein_id
)

select * from joined;
