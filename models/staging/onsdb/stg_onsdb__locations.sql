with bron as (

    select * from {{ source('ons_plan_2', 'locations') }}

),

definitief as (

    select
        objectId                as locatie_id,
        name                    as locatienaam,
        identificationNo        as identificatienummer,
        agbCode                 as agb_code,
        wzaCode                 as wza_code,
        intramuralLocation      as is_intramuraal,
        capacity                as capaciteit,
        parentObjectId          as ouder_locatie_id,
        addressObjectId         as adres_id,
        materializedPath        as locatie_hierarchie_pad,
        cast(beginDate as date) as startdatum_locatie,
        cast(endDate as date)   as einddatum_locatie,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
