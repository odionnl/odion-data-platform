with bron as (

    select * from {{ source('ons_plan_2', 'team_assignments') }}

),

definitief as (

    select
        objectId                as teamkoppeling_id,
        employeeObjectId        as medewerker_id,
        teamObjectId            as team_id,
        beginDate               as startdatum,
        endDate                 as einddatum,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
