with bron as (

    select * from {{ source('ons_plan_2', 'care_order_products') }}

),

definitief as (

    select
        objectId                as zorglegitimatie_product_id,
        careOrderObjectId       as zorglegitimatie_id,
        productObjectId         as product_id,
        beginDateClipped        as startdatum,
        endDateClipped          as einddatum,
        beginDate               as startdatum_origineel,
        endDate                 as einddatum_origineel,
        quantityInMinutes       as hoeveelheid_in_minuten,
        awbzKlasseObjectId      as awbz_klasse_id,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
