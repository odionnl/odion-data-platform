with bron as (

    select * from {{ source('ons_plan_2', 'locations') }}

),

locatietypen as (

    select * from {{ ref('stg_onsdb__lst_location_types') }}

),

definitief as (

    select
        bron.objectId                as locatie_id,
        bron.name                    as locatienaam,
        bron.identificationNo        as identificatienummer,
        bron.agbCode                 as agb_code,
        bron.wzaCode                 as wza_code,
        bron.intramuralLocation      as is_intramuraal,
        bron.capacity                as capaciteit,
        bron.parentObjectId          as ouder_locatie_id,
        bron.addressObjectId         as adres_id,
        bron.materializedPath        as locatie_hierarchie_pad,
        cast(bron.beginDate as date) as startdatum_locatie,
        cast(bron.endDate as date)   as einddatum_locatie,
        locatietypen.locatietype,
        bron.createdAt               as aangemaakt_op,
        bron.updatedAt               as gewijzigd_op

    from bron
    left join locatietypen
        on locatietypen.locatietype_code = bron.[type]

)

select * from definitief
