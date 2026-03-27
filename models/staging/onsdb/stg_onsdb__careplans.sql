with bron as (

    select * from {{ source('ons_plan_2', 'careplans') }}

),

definitief as (

    select
        objectId            as zorgplan_id,
        clientObjectId      as client_id,
        employeeObjectId    as aangemaakt_door_id,
        beginDate           as startdatum,
        endDate             as einddatum,
        status              as status_code,
        createdAt           as aangemaakt_op,
        updatedAt           as gewijzigd_op

    from bron

)

select * from definitief
