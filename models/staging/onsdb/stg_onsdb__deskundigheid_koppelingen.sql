with bron as (

    select * from {{ source('ons_plan_2', 'expertise_profile_assignments') }}

),

definitief as (

    select
        employeeObjectId         as medewerker_id,
        expertiseProfileObjectId as deskundigheid_id,
        startTime                as startdatum,
        endTime                  as einddatum

    from bron

)

select * from definitief
