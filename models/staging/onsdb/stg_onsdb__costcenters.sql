with bron as (

    select * from {{ source('ons_plan_2', 'costcenters') }}

),

definitief as (

    select
        objectId            as kostenplaats_id,
        identificationNo    as kostenplaats_code,
        name                as kostenplaats_naam

    from bron

)

select * from definitief
