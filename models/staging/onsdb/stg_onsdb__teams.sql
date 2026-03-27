with bron as (

    select * from {{ source('ons_plan_2', 'teams') }}

),

definitief as (

    select
        objectId                as team_id,
        name                    as teamnaam,
        identificationNo        as identificatienummer,
        poolTeam                as is_poolteam,
        parentObjectId          as bovenliggend_team_id,
        materializedPath        as pad,
        beginDate               as startdatum,
        endDate                 as einddatum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
