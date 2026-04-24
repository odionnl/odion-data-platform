with bron as (

    select * from {{ source('ons_plan_2', 'presence_logs') }}

),

definitief as (

    select
        objectId                        as zorgregel_id,
        activityObjectId                as activiteit_id,
        employeeId                      as medewerker_id,
        clientId                        as client_id,
        costClusterObjectId             as team_id,
        startDate                       as starttijd,
        endDate                         as eindtijd,
        origStartDate                   as starttijd_origineel,
        origEndDate                     as eindtijd_origineel,
        loggedStartDate                 as starttijd_geregistreerd,
        loggedEndDate                   as eindtijd_geregistreerd,
        cast(hasDuration as int)        as heeft_tijdsduur,
        cast(isAutomaticallyDivided as int) as is_automatisch_verdeeld,
        cast(verified as int)           as is_gefiatteerd,
        verifiedDate                    as gefiatteerd_op,
        cast(registration as int)       as is_urenregistratie,
        cast(payment as int)            as is_verloning,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

    where removed = 0  -- verwijderde zorgregels uitsluiten

)

select * from definitief
