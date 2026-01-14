with 

administration_agreements as (
    select 
    * 
    from {{ ref('stg_ons__administration_agreements') }}
),

medication_administrations as (
    select 
    * 
    from {{ ref('stg_ons__medication_administrations') }}
),

medication_charts as (
    select 
    * 
    from {{ ref('stg_ons__medication_charts') }}
),

status_updates as (
    select 
    * 
    from {{ ref('stg_ons__status_updates') }}
),

final as (

    select
      mc.client_id,
      mc.toedienlijst_id,
      ma.toediening_id,
      aa.toedienafspraak_id,
      ma.gepland_op,
      su.nieuwe_status,
      su.aangemaakt_op,
      mc.fake,
      ma.vrijgesteld
    from medication_administrations ma
    left join administration_agreements aa
        on aa.toedienafspraak_id=ma.toedienafspraak_id
    left join status_updates su
        on su.toediening_id=ma.toediening_id
    left join medication_charts mc
        on mc.toedienlijst_id=aa.toedienlijst_id
)

select * from final;
