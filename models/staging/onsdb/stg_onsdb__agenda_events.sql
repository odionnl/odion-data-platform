-- Ons Agenda: afspraken en afspraakreeksen.
-- De opmerking (comment) wordt bewust niet overgenomen: vrije tekst met mogelijk gezondheidsinformatie (AVG, dataminimalisatie).

with bron as (

    select * from {{ source('ons_plan_2', 'agenda_events') }}

),

definitief as (

    select
        objectId                        as afspraak_id,
        timelineId                      as reeks_id,
        [name]                          as titel,
        startTime                       as starttijd_seconden,
        duration                        as duur_seconden,
        hourTypeObjectId                as agenda_uursoort_id,
        customLocation                  as locatie_omschrijving,
        cast(clientPresent as int)      as is_client_aanwezig,
        cast([private] as int)          as is_prive,
        recurrenceType                  as herhalingstype,
        validFrom                       as geldig_vanaf,
        validTo                         as geldig_tot,
        cycleBase                       as herhaling_basisdatum,
        cycleInterval                   as herhaling_interval,
        cast([exception] as int)        as is_uitzondering,
        cast(monday as int)             as is_maandag,
        cast(tuesday as int)            as is_dinsdag,
        cast(wednesday as int)          as is_woensdag,
        cast(thursday as int)           as is_donderdag,
        cast(friday as int)             as is_vrijdag,
        cast(saturday as int)           as is_zaterdag,
        cast(sunday as int)             as is_zondag,
        [source]                        as bron_applicatie,
        creatorObjectId                 as aangemaakt_door_id,
        updaterObjectId                 as gewijzigd_door_id,
        createdAt                       as aangemaakt_op,
        updatedAt                       as gewijzigd_op

    from bron

)

select * from definitief
