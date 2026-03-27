with bron as (

    select * from {{ source('ons_plan_2', 'care_arrangements') }}

),

definitief as (

    select
        objectId                as zorgarrangement_id,
        clientObjectId          as client_id,
        activityObjectId        as activiteit_id,
        beginDate               as startdatum,
        endDate                 as einddatum,
        normMorning             as minuten_ochtend,
        normAfternoon           as minuten_middag,
        normEvening             as minuten_avond,
        normNight               as minuten_nacht,
        mornings                as weekdagen_ochtend,
        afternoons              as weekdagen_middag,
        evenings                as weekdagen_avond,
        nights                  as weekdagen_nacht,
        createdAt               as aangemaakt_op,
        updatedAt               as gewijzigd_op

    from bron

)

select * from definitief
