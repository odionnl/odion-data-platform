with bron as (

    select * from {{ source('ons_plan_2', 'care_allocations') }}

),

definitief as (

    select
        objectId                as zorgtoewijzing_id,
        clientObjectId          as client_id,
        dateBegin               as startdatum,
        dateEnd                 as einddatum,
        outOfCareReason         as reden_uit_zorg,
        outOfCareDestination    as bestemming_uit_zorg,
        comments                as opmerkingen,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
