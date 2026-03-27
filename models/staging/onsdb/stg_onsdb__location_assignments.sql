with bron as (

    select * from {{ source('ons_plan_2', 'location_assignments') }}

),

definitief as (

    select
        objectId                as locatiekoppeling_id,
        clientObjectId          as client_id,
        locationObjectId        as locatie_id,
        beginDate               as startdatum,
        endDate                 as einddatum,
        locationType            as type_toekenning,
        residence               as is_verblijfslocatie,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
