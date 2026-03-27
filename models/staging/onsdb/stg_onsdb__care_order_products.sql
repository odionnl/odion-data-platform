with bron as (

    select * from {{ source('ons_plan_2', 'care_order_products') }}

),

definitief as (

    select
        objectId                as zorglegitimatieproduct_id,
        careOrderObjectId       as zorglegitimatie_id,
        productObjectId         as product_id,
        beginDate               as startdatum,
        endDate                 as einddatum,
        beginDateClipped        as ingangsdatum_geldig,
        endDateClipped          as einddatum_geldig,
        quantityInMinutes       as hoeveelheid_in_minuten,
        awbzKlasseObjectId      as awbz_klasse_id,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
